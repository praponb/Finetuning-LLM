from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(10400, 14900))
    .withColumn("least_credit_limit_age", F.least("credit_limit", "age"))
    .withColumn("age_tier", F.when(F.col("age") >= 58, "High").when(F.col("age") >= 26, "Medium").otherwise("Low"))
    .orderBy("signup_date", "id")
    .limit(3)
)
