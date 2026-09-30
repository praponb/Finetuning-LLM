from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("cost").alias("value"), F.lit("product").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("shipping_fee").alias("value"), F.lit("order").alias("source"))
    )
)
