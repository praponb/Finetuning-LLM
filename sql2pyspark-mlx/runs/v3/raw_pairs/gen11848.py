from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") | F.col("end_date").between("2024-02-01", "2025-08-01"))
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"))
)
