from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 4.4, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.col("ss.units")).alias("total_units"))
    .orderBy("total_units", "rating_bucket")
)
