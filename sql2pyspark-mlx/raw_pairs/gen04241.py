from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.brand") == "Umbrella")
    .groupBy(
        F.when(F.col("s.rating") >= 3.8, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(F.countDistinct(F.col("p.price")).alias("unique_brands"))
    .orderBy("country")
)
