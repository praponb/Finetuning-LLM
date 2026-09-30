from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .cube(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.units")), 2).alias("total_units"),
    )
)
