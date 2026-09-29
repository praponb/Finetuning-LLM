from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .groupBy("store_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.min("revenue").alias("min_revenue"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("max_units") < 21)
    .drop("max_units")
    .orderBy("min_revenue")
)
