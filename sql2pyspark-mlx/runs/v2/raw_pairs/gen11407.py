from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 11)
    .select(
        "store_id",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled_2"),
    )
)
