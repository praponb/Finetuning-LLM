from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), F.col("id").desc())

filtered = (
    df_policies
    .filter((F.col("holder_id") <= 16) & (F.col("start_date") >= F.lit("2024-05-07").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.days_end_date_to_start_date",
        "s.premium_filled",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") <= 2945.9) & (~F.col("p.product").isin("TRAVEL", "LIFE")), "left_anti")
)

latest = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

summary = (
    latest
    .filter(F.col("holder_joined_on").isNotNull())
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        "holder_id",
    )
    .agg(
        F.max("premium_filled").alias("max_premium_filled"),
        F.max("start_date").alias("last_start_date"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("end_date").alias("first_end_date"),
        F.sum("days_end_date_to_start_date").alias("sum_days_end_date_to_start_date_check"),
    )
    .filter(F.col("sum_days_end_date_to_start_date_check") > -560)
    .drop("sum_days_end_date_to_start_date_check")
)

with_details_holder = (
    summary.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.end_date_month",
        "s.start_date_ym",
        "s.holder_id",
        "s.max_premium_filled",
        "s.last_start_date",
        "s.row_count",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

qualified = (
    with_details_holder
    .crossJoin(with_details_holder.agg(F.max("row_count").alias("overall_max_row_count")))
    .filter(F.col("row_count") < F.col("overall_max_row_count"))
    .drop("overall_max_row_count")
)

result = (
    qualified
    .groupBy("last_start_date")
    .pivot("holder_segment", ["VIP", "SME", "RETAIL"])
    .agg(F.count("holder_risk_score"))
    .orderBy("last_start_date")
)
