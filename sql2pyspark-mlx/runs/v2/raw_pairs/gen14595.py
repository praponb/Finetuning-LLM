from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 13) | (F.col("amount") < 540))
    .select(
        "id",
        "order_date",
        "store_id",
        "shipping_fee",
        (F.col("amount") + F.col("discount")).alias("amount_plus_discount"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
