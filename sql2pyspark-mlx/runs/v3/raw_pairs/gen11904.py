from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("shipping_fee").alias("value"))
    .unionByName(
        df_products
        .filter(F.col("cost").between(280, 490))
        .select(F.col("price").alias("value"))
    )
    .orderBy("value")
)
