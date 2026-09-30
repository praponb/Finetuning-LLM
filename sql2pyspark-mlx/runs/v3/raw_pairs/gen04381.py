from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("max_revenue", "product_id")
    .limit(20)
)
