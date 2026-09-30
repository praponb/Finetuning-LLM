from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(540, 880) & F.col("brand").isin(["Globex", "Stark"]))
    .select(
        "id",
        "price",
        F.when(F.col("category").isin(["Grocery", "Home"]), "Priority").otherwise("Other").alias("category_group"),
    )
)
