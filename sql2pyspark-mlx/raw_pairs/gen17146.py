from pyspark.sql import functions as F

src = df_store_sales.filter(F.year("sale_date") == 2024)

revenue_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("revenue") >= 3200, "high").when(F.col("revenue") >= 1400, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("*").alias("cnt"),
    )
)

result = (
    revenue_bucket_summary
    .filter(F.col("cnt") < 4)
    .select("revenue_bucket", "units_range", "cnt")
    .orderBy("units_range")
)
