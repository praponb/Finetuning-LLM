from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("credit_limit").isNull())

agg = (
    recent_customers
    .groupBy("segment")
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.count("*").alias("customer_count"),
    )
)

result = agg
