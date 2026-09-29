from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "JP"]))
    .groupBy("segment", "city")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
        F.sum("age").alias("total_age"),
        F.count("*").alias("customer_count"),
    )
    .filter(F.col("customer_count") > 3)
    .drop("customer_count")
    .orderBy("segment", "city")
    .limit(20)
)
