from pyspark.sql import functions as F

valid_customers = df_customers.filter(~(F.col("email").like("%@GMAIL.COM") & (F.col("age") < 55)))

summary = (
    valid_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.max("credit_limit").alias("max_credit_limit"))
)

result = summary.select("signup_year", "max_credit_limit")
