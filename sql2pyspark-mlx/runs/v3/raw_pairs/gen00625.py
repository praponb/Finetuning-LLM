from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "tags",
        "price",
        F.when(F.col("category").isin(["Home", "Fashion"]), "Core").otherwise("Other").alias("category_group"),
    )
)
