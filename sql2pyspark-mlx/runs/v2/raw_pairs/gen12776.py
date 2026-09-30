from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter((F.col("credit_limit") >= 6500) & (F.col("credit_limit") <= 4700))
)

summary = (
    valid_customers
    .groupBy("country")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
)

result = (
    summary
    .select("country", "age_range")
    .orderBy(F.col("age_range").desc())
)
