from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("age") <= 54) & (F.col("country") == "US") & F.col("city").isNull())
)

segment_summary = (
    base
    .groupBy("segment")
    .agg(F.countDistinct("age").alias("unique_citys"))
)

result = segment_summary.orderBy("segment")
