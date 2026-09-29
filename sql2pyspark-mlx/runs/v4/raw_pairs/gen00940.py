from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 4.4, "high").when(F.col("s.rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("p.brand") == "Globex", F.col("p.price")).otherwise(0)).alias("globex_price"),
    )
    .orderBy("rating_bucket", "globex_price")
)
