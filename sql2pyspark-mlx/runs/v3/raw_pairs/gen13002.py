from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") == 63) & (F.col("age") <= 39))
    .groupBy("is_active")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .orderBy(F.col("age_range").desc(), "is_active")
    .limit(3)
)
