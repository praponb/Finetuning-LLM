from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 3) & (F.col("sale_date") >= "2023-06-01"))
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("units").alias("units_count"),
    )
    .orderBy("units_count")
)
