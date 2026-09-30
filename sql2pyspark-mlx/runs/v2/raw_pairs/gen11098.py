from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.5, "high").when(F.col("suppliers.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("products.category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
        F.count(F.when(F.col("products.brand") == "Umbrella", 1)).alias("umbrella_count"),
    )
    .orderBy("umbrella_count")
)
