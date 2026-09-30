from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_year_by_pre = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_start_date_month = Window.partitionBy("holder_id", F.month("start_date"))
w_all_by_premium = Window.orderBy("premium", "id")
w_start_date_year_by_holder_ri = Window.partitionBy(F.year("start_date")).orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(-2, -1)
w_end_date_month_holder_id = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id")
w_start_date_year_by_holder_ri_2 = Window.partitionBy(F.year("start_date")).orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_by_holder_ri_3 = Window.partitionBy(F.year("start_date")).orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(-2, Window.currentRow)

scoped_policys = (
    df_policies
    .filter(~F.col("product").isin("HOME", "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

joined_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    joined_holder
    .select(
        "*",
        F.last("premium", ignorenulls=True).over(w_product_end_date_year_by_pre).alias("last_known_premium"),
        F.sum("premium").over(w_all_by_premium_running).alias("running_premium"),
        F.max("holder_risk_score").over(w_product_holder_id_by_premium).alias("max_holder_risk_score_so_far"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_holder_id_start_date_month), 2).alias("holder_risk_score_vs_holder_id_start_date_month_"),
        F.dense_rank().over(w_all_by_premium).alias("premium_dense_rank"),
        F.max("premium").over(w_start_date_year_by_holder_ri).alias("premium_max_2_prior"),
        F.min("premium").over(w_end_date_month_holder_id).alias("end_date_month_holder_id_min_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_end_date_month_holder_id), F.lit(0)), 4).alias("premium_share_of_end_date_month_holder_id"),
        F.nth_value("end_date", 2).over(w_start_date_year_by_holder_ri_2).alias("second_end_date"),
        F.round(F.avg("premium").over(w_start_date_year_by_holder_ri_3), 2).alias("premium_avg_3"),
    )
)

top_per_group = windowed.filter(F.col("premium_dense_rank") <= 2)

result = (
    top_per_group
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "holder_risk_score",
        "last_known_premium",
        "running_premium",
        "max_holder_risk_score_so_far",
        "premium_dense_rank",
        "premium_share_of_end_date_month_holder_id",
        "second_end_date",
        "premium_avg_3",
    )
    .orderBy(F.col("max_holder_risk_score_so_far").desc())
)
