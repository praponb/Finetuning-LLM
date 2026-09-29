from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2024-07-19")
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("total_units") > 70)
)
