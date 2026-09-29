from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating") > 3.4)
    .groupBy(
        F.when(F.col("s.rating") >= 4.2, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg(F.col("ss.units")).alias("avg_units"),
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
    )
    .orderBy("avg_units")
)
