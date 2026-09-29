from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.size("tags") > 1) & F.col("supplier_id").isNull()) | ~F.col("category").isin(["Electronics", "Sports", "Books"]))
    .select(
        "id",
        "price",
        "supplier_id",
        F.coalesce("brand", F.lit("unassigned")).alias("brand_filled"),
    )
    .orderBy("price", F.col("id").desc())
)
