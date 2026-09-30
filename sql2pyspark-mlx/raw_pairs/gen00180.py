from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-08-20"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.ceil("amount").alias("amount_whole"),
        F.upper("status").alias("status_upper"),
    )
)

result = (
    base
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1700, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy(F.col("status").asc_nulls_last())
)
