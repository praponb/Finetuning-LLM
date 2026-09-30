from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") < 0.39) & (F.col("discount") < 0.14) & F.col("amount").between(640, 960))
    .select(
        "shipping_fee",
        "store_id",
        "order_date",
        F.date_trunc("month", "order_date").alias("order_date_period"),
    )
    .orderBy(F.col("order_date_period").desc())
)
