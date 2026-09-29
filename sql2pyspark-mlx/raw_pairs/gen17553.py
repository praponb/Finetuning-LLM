from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.category") == "Grocery")
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(F.sum(F.col("p.price")).alias("total_price"))
)
