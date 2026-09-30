from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(8600, 15600) & F.col("city").isin(["New York", "Bangkok"]) & F.col("email").isNotNull())
    .groupBy(
        F.when(F.col("credit_limit") >= 12700, "high").when(F.col("credit_limit") >= 7800, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.quarter("signup_date").alias("signup_quarter"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
    )
    .filter(F.col("us_credit_limit") < 45)
    .drop("us_credit_limit")
    .orderBy("signup_quarter", "unique_countrys")
)
