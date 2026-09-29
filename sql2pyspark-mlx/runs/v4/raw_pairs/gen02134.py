from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-11-16"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.when(F.col("amount") >= 1470, "High").when(F.col("amount") >= 670, "Medium").otherwise("Low").alias("amount_level"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

result = (
    cleaned
    .select("id")
    .subtract(
        df_payments
        .filter(F.to_date("paid_at") < "2024-07-13")
        .select("id")
    )
)
