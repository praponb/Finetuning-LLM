from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .subtract(
        df_products
        .filter(F.col("supplier_id") == 30)
        .select(F.col("cost").alias("shipping_fee"))
    )
)
