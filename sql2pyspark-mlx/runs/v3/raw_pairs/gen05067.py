from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 630) & (F.col("channel").isNull() | (F.col("order_date") >= "2025-05-24")))
    .select(
        "id",
        "discount",
        "channel",
        F.date_trunc("MONTH", "order_date").alias("order_date_period"),
    )
    .orderBy(F.col("id").desc(), "order_date_period")
)
