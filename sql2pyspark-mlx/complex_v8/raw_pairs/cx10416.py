from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_all_by_premium_ties = Window.orderBy("premium")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_premium_prior_6 = Window.orderBy("premium", "id").rowsBetween(-6, -1)
w_holder_segment_by_premium_ru = Window.partitionBy("holder_segment").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_prior_1 = Window.orderBy("end_date", "id").rowsBetween(-1, -1)

prepared = (
    df_policies
    .filter((F.col("premium") > 1132.1) & (F.col("status") != "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.product_ref",
        "s.start_date_month_end",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_product), 2).alias("holder_risk_score_vs_product_avg"),
        F.dense_rank().over(w_all_by_premium_ties).alias("premium_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_end_date), 3).alias("end_date_cume_dist"),
        F.sum("premium").over(w_all_by_premium_prior_6).alias("premium_sum_6_prior"),
        F.sum("premium").over(w_holder_segment_by_premium_ru).alias("running_premium"),
        F.round(F.avg("premium").over(w_all_by_end_date_prior_1), 2).alias("premium_avg_1_prior"),
        F.min("premium").over(w_product).alias("product_min_premium"),
        F.last("end_date", ignorenulls=True).over(w_holder_segment_by_premium_ru).alias("last_known_end_date"),
    )
)

result = (
    with_trends
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "product_ref",
        "holder_risk_score",
        "premium_vs_product_avg",
        "holder_risk_score_vs_product_avg",
        "premium_dense_rank",
        "end_date_cume_dist",
        "running_premium",
        "premium_avg_1_prior",
        "product_min_premium",
        F.coalesce(F.col("holder_segment"), F.lit("UNKNOWN")).alias("holder_segment_or_unknown"),
    )
    .orderBy(
        F.col("running_premium").desc_nulls_first(),
        F.col("premium_vs_product_avg").desc(),
    )
)
