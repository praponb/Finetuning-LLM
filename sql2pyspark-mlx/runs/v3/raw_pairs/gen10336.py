from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("units") <= 14)

units_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 13, "mid").otherwise("low").alias("units_bucket"),
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
    )
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)

result = (
    units_bucket_summary
    .filter(F.col("unique_stores") >= 7)
    .orderBy(F.col("unique_stores").desc(), "units_bucket", "sale_month")
    .limit(3)
)
