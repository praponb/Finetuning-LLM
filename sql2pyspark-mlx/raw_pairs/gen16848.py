from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("product_name")
    .unionByName(
        df_employees
        .filter(F.col("level").isNotNull())
        .select(F.col("level").alias("product_name"))
    )
    .distinct()
)
