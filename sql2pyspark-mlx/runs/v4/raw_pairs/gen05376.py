from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("age_tier", F.when(F.col("age") >= 58, "PREMIUM").otherwise("STANDARD"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 15200, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["age_tier", "credit_limit_tier"], ["age_tier"]],
        "age_tier",
        "credit_limit_tier",
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("age")).otherwise(0)).alias("chiang_mai_age"),
        F.avg(F.when(F.col("city") == "London", F.col("credit_limit"))).alias("london_avg_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.grouping("age_tier").alias("grouping_id"),
    )
)
