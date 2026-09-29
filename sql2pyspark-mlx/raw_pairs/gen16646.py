from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)
