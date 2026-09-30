from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("*", F.size("tags").alias("tags_count"))
)

result = (
    filtered
    .select(
        "id",
        "category",
        F.col("price").alias("PRICE_VAL"),
        F.col("cost").alias("COST"),
    )
    .unpivot(["id", "category"], ["PRICE_VAL", "COST"], "metric_name", "reading")
    .filter(F.col("reading").isNotNull())
)
