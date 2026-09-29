from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy("total_revenue")
)
