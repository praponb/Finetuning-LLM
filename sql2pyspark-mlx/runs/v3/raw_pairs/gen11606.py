from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("s.country").isin(["UK", "US", "JP"]))
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
)
