from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.category").isin(["Grocery", "Sports", "Fashion"]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.count("*").alias("total_count"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
)
