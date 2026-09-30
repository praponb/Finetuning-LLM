from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2024-03-02 07:00:00"))) & F.col("order_id").isNotNull() & F.col("method").isin(["card", "cash", "bank_transfer"]))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

result = (
    base
    .groupBy(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        "method",
        "order_id",
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.min("paid_at").alias("first_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("num_methods") >= 1)
    .orderBy(F.col("min_amount").desc_nulls_last())
)
