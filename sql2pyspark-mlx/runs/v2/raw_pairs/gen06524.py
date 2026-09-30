from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("email") >= 8) & F.col("city").isNotNull())
    .groupBy("is_active")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
)
