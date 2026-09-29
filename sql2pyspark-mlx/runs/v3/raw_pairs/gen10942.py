from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

signup_year_summary = (
    valid_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = (
    signup_year_summary
    .select("signup_year", "total_credit_limit", "max_credit_limit")
    .orderBy("max_credit_limit", "signup_year")
    .limit(20)
)
