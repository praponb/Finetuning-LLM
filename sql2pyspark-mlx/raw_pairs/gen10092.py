from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("sale_date") <= "2023-08-01")

store_id_summary = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    store_id_summary
    .filter(F.col("stddev_revenue") > 30)
    .orderBy("stddev_revenue", F.col("store_id").desc())
)
