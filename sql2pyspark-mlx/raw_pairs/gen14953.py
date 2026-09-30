from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(54, 56) & (F.col("segment").isin(["Corporate", "Consumer"]) | ~F.col("name").like("%Ali%")))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg((F.max("age") - F.min("age")).alias("age_range"))
)
