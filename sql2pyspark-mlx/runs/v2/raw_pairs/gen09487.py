from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.supplier_name").like("To%"))
    .groupBy(
        F.when(F.col("s.rating") >= 4.1, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(
        F.count(F.when(F.col("p.category") == "Grocery", 1)).alias("grocery_count"),
        F.count(F.col("p.cost")).alias("cost_count"),
    )
)
