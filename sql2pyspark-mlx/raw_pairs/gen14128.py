from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-10-18"))) & F.col("order_id").isin([9, 19]))
    .select(
        "*",
        F.length("method").alias("method_length"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("method_length") >= 65, "PREMIUM").otherwise("STANDARD").alias("method_length_tier"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.avg(F.when(F.col("status") == "FAILED", F.col("method_length"))).alias("failed_avg_method_length"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .orderBy("method_length_tier")
)
