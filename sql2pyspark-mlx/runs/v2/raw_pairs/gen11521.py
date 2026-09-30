from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .orderBy(F.col("latest_sale_date").desc(), F.col("revenue_range").desc())
)
