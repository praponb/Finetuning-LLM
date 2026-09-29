from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.month("sale_date").alias("sale_month"))
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.max("units").alias("max_units"),
        F.avg("units").alias("avg_units"),
    )
)
