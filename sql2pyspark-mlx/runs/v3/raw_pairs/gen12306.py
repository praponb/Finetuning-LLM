from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("signup_date_label", F.date_format("signup_date", "MMM yyyy"))
    .withColumn("email_domain", F.regexp_extract("email", "@(.+)$", 1))
)
