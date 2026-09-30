from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("cost").alias("value"))
    .unionByName(
        df_products
        .filter(F.col("cost") <= 200)
        .select(F.col("price").alias("value"))
    )
)
