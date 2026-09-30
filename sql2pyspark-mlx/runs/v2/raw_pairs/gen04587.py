from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.month("sale_date").alias("sale_month"))
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)
