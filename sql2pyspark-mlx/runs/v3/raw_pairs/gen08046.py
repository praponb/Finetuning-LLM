from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Fashion"]) & (F.length("product_name") < 9))
    .select(
        "cost",
        "supplier_id",
        "category",
        F.size("tags").alias("tags_count"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
    .orderBy(F.col("tags_count").desc())
)
