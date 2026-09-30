from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("country").isin(["DE", "UK"]), "group_a").otherwise("other").alias("country_group"),
        F.quarter("signup_date").alias("signup_quarter"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
    .filter(F.col("consumer_count") > 6)
    .select("country_group", "signup_quarter", "row_count", "consumer_count")
)
