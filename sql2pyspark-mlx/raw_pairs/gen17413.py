from pyspark.sql import functions as F

result = (
    df_customers
    .select("name")
    .unionByName(
        df_products
        .filter(~F.col("product_name").like("%Mak%"))
        .select(F.col("product_name").alias("name"))
    )
)
