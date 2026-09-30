from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "cost",
        "price",
        "launch_date",
        F.lower("category").alias("category_lower"),
        F.when(F.col("brand") == "Stark", 1).otherwise(0).alias("is_stark"),
    )
    .orderBy(F.col("price").desc())
)
