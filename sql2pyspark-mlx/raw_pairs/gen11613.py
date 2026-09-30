from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isNotNull())
    .select(F.col("category").alias("label"))
    .subtract(
        df_suppliers
        .filter(F.col("country").isNotNull())
        .select(F.col("country").alias("label"))
    )
    .orderBy(F.col("label").desc())
)
