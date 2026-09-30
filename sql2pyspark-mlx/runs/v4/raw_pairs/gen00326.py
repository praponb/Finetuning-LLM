from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull() | F.col("city").isin(["London", "Chiang Mai", "Tokyo"]))
    .groupBy("country")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .orderBy("country")
    .limit(5)
)
