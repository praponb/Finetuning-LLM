from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("s.country").isin(["JP", "TH"]))
    .groupBy(
        F.when(F.col("s.rating") >= 4.6, "high").when(F.col("s.rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
        F.min(F.col("ss.revenue")).alias("min_revenue"),
    )
)
