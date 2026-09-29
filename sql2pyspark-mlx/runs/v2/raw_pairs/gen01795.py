from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Sports"]))
    .select(
        "id",
        "tags",
        "brand",
        F.when(F.col("brand").isin(["Acme", "Umbrella"]), "Group A").otherwise("Other").alias("brand_group"),
    )
)
