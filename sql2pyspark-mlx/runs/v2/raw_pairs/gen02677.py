from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("country") == "JP")

agg = (
    filtered_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
)

result = (
    agg
    .filter(F.col("corporate_count") >= 74)
    .orderBy("corporate_count")
)
