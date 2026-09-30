from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter(F.col("revenue") < 3700)

store_id_summary = (
    valid_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    store_id_summary
    .select("store_id", "stddev_units")
    .orderBy("stddev_units", "store_id")
)
