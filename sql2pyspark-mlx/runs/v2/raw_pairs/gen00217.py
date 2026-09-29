from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1030) & F.col("order_id").isin([14, 24, 38]))
    .select(
        "id",
        "order_id",
        F.concat("status", F.lit(" - "), "method").alias("status_method"),
        F.ceil("amount").alias("amount_whole"),
    )
)
