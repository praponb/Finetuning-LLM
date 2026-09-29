from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 2, 3, 5]) & F.col("s.store_name").like("Dow%"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.when(F.col("s.region").isin(["West", "East"]), "Tier 1").otherwise("Other").alias("region_group"),
    )
)
