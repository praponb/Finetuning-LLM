from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter((F.col("revenue") > 3900) & (F.col("units") >= 14))

agg = (
    recent_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("units").alias("units_count"),
    )
)

result = (
    agg
    .filter(F.col("units_count") > 3)
    .select("sale_month", "units_count")
    .orderBy(F.col("sale_month").desc(), F.col("units_count").desc())
)
