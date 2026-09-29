from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1500)
    .select(
        "id",
        "order_date",
        "store_id",
        F.round((F.col("shipping_fee") * 100), 2).alias("shipping_fee_scaled"),
    )
)
