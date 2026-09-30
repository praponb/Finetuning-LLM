from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .filter(F.col("ss.revenue") >= 2300)
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
        F.count("*").alias("store_sale_count"),
    )
)
