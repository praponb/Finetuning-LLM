from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("credit_limit") < 14800)
    .withColumn("name_filled", F.coalesce("name", F.lit("Unknown")))
)

summary = (
    filtered_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.max("signup_date").alias("latest_signup_date"),
    )
)

result = (
    summary
    .filter(F.col("total_credit_limit") < 55000)
    .select("signup_year", "latest_signup_date")
)
