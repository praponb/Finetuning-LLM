from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        (F.col("amount") / F.nullif("shipping_fee", F.lit(0))).alias("amount_per_shipping_fee"),
    )
)
