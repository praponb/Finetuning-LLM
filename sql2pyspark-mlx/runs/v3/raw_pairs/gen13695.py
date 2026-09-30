from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2023)
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.countDistinct("units").alias("unique_stores"),
    )
    .filter(F.col("revenue_count") > 4)
    .select("sale_month", "revenue_count", "unique_stores")
    .orderBy(F.col("sale_month").desc())
)
