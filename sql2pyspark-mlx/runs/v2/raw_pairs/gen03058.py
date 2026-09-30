from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 550) & F.col("status").isin(["CANCELLED", "RETURNED", "SHIPPED"]))
    .select(
        "id",
        "amount",
        F.upper("channel").alias("channel_upper"),
        (F.col("discount") * F.col("amount")).alias("discount_times_amount"),
    )
    .orderBy(F.col("channel_upper").desc(), "id")
)
