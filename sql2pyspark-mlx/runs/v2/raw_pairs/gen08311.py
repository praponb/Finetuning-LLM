from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 4.1, "high").when(F.col("s.rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(F.max(F.col("p.stock_qty")).alias("max_stock_qty"))
    .orderBy("max_stock_qty")
)
