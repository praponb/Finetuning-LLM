from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("name").like("%Emma%") & F.col("is_active"))
    .withColumn("signup_date_label", F.date_format("signup_date", "yyyy-MM-dd"))
)

summary = (
    valid_customers
    .groupBy("is_active")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.sum("age").alias("total_age"),
    )
)

result = (
    summary
    .filter(F.col("max_credit_limit") > 15200)
    .select("is_active", "max_credit_limit", "total_age")
    .orderBy(F.col("max_credit_limit").desc(), F.col("total_age").desc())
)
