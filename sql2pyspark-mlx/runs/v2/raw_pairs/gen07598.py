from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("credit_limit") < 5300))
    .withColumn("email_country_key", F.concat_ws("-", "email", "country"))
    .withColumn("name_title", F.initcap(F.trim("name")))
)
