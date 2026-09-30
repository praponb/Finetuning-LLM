from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select(F.lit("customer").alias("record_type"), F.col("email").alias("label"))
    .unionByName(
        df_products
        .filter(F.col("category").isNotNull())
        .select(F.lit("product").alias("record_type"), F.col("category").alias("label"))
    )
    .distinct()
    .orderBy(F.col("label").desc())
)
