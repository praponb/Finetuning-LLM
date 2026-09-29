from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 16) & (F.year("sale_date") >= 2025))
    .groupBy("store_id")
    .agg(
        F.max("units").alias("max_units"),
        F.count("*").alias("num_store_sales"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)
