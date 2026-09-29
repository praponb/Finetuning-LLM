from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 34)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
)
