from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_holder = Window.partitionBy("holder_id", "product").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id")
w_product_holder_id_by_holder = Window.partitionBy("product", "holder_id").orderBy("holder_risk_score", "id")
w_product_holder_id_by_holder_2 = Window.partitionBy("product", "holder_id").orderBy("holder_risk_score", "id").rowsBetween(-2, -1)
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(-2, -1)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(-6, Window.currentRow)
w_status_product_by_start_date = Window.partitionBy("status", "product").orderBy(F.col("start_date").desc())
w_status_product_by_start_date_2 = Window.partitionBy("status", "product").orderBy(F.col("start_date").desc(), "id")
w_status = Window.partitionBy("status")
w_product = Window.partitionBy("product")
w_holder_id_product_by_holder_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_risk_score_ran = Window.partitionBy("status", "holder_risk_score_rank").orderBy("holder_risk_score_max_2_prior_2")
w_holder_id_next_start_date_mo = Window.partitionBy("holder_id", F.month("next_start_date"))

scoped_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-07-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

joined = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.cume_dist().over(w_holder_id_product_by_holder), 3).alias("holder_risk_score_cume_dist"),
        F.rank().over(w_product_holder_id_by_holder).alias("holder_risk_score_rank"),
        F.max("holder_risk_score").over(w_product_holder_id_by_holder_2).alias("holder_risk_score_max_2_prior"),
        F.max("holder_risk_score").over(w_status_holder_id_by_premium).alias("holder_risk_score_max_2_prior_2"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.round(F.avg("holder_risk_score").over(w_status_holder_id_by_premium_2), 2).alias("holder_risk_score_avg_7"),
        F.round(F.percent_rank().over(w_status_product_by_start_date), 3).alias("start_date_percent_rank"),
        F.row_number().over(w_status_product_by_start_date_2).alias("rn"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.lead("start_date").over(w_all_by_start_date).alias("next_start_date"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_product), F.lit(0)), 4).alias("holder_risk_score_share_of_product"),
        F.sum("holder_risk_score").over(w_holder_id_product_by_holder_2).alias("running_holder_risk_score"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.cume_dist().over(w_status_holder_risk_score_ran), 3).alias("holder_risk_score_max_2_prior_2_cume_dist"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_id_next_start_date_mo), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_id_next_start_"),
        F.round(F.avg("running_holder_risk_score").over(w_holder_id_next_start_date_mo), 2).alias("holder_id_next_start_date_month_avg_running_hold"),
    )
)

grouped = (
    windowed
    .filter(F.col("holder_risk_score_share_of_holder_id_next_start_") > 0.2)
    .groupBy(
        "rn",
        F.date_trunc("month", "next_start_date").alias("next_start_date_month"),
    )
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("product") != "HOME", 1)).alias("n_product_home"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
)

result = (
    grouped
    .select(
        "rn",
        "next_start_date_month",
        "n_status_is_not_null",
        "n_product_home",
        "distinct_holder_ids",
    )
    .orderBy(F.col("n_status_is_not_null").desc(), "rn")
)
