from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("category").isin(["Grocery", "Fashion", "Books"]))
    .groupBy(
        F.when(F.col("category").isin(["Sports", "Electronics"]), "core").otherwise("other").alias("category_group"),
        "category",
        "brand",
    )
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy("category_group", "category", "brand")
    .limit(3)
)
