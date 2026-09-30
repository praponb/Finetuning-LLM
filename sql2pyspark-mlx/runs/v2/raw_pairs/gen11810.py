from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 16) & (F.col("sale_date") <= "2025-10-17"))
    .groupBy("store_id", F.year("sale_date").alias("sale_year"))
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("units").alias("units_count"),
    )
    .orderBy("sale_year")
)
