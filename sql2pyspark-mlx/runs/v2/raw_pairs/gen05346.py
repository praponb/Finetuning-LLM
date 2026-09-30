from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "discount",
        "order_date",
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
    )
)
