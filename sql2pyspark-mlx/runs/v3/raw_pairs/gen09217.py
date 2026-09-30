from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.revenue") < 3700)
    .groupBy(
        F.when(F.col("s.region").isin(["West", "Central"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
    .orderBy(F.col("revenue_count").desc(), "region_group")
)
