from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.units") <= 12)
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("latest_sale_date").desc())
)
