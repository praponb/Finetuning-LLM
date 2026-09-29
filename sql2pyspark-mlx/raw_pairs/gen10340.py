from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").isNotNull())
    .select(F.col("launch_date").alias("event_date"))
    .subtract(
        df_store_sales
        .filter(F.col("sale_date").isNotNull())
        .select(F.col("sale_date").alias("event_date"))
    )
    .orderBy("event_date")
)
