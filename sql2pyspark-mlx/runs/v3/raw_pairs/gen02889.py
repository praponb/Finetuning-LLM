from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") < "2023-03-06") & (F.col("shipping_fee") <= 34))
    .select(
        "id",
        "amount",
        "shipping_fee",
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy("shipping_fee")
)
