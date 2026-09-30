from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") != "London")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.countDistinct(F.col("ss.units")).alias("unique_products"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc())
)
