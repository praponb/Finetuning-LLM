from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-05-03").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.trim("method").alias("clean_method"),
    )
)

result = (
    cleaned
    .groupBy("method")
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("amount_per_bank_transfer"),
        F.count("*").alias("cnt"),
    )
)
