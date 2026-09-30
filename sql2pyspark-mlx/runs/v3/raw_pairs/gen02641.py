from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("country") == "JP")

agg = (
    filtered_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0))) / F.count("*")), 1).alias("pct_corporate"),
    )
)

result = agg.orderBy("pct_corporate", "signup_month")
