from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") > "2025-06-13") & (F.col("sqft") > 7784))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.min("opened_date").alias("min_opened_date"))
)
