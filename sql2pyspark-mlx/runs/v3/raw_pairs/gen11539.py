from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", F.lit("store").alias("source"))
    .union(df_customers.select("signup_date", F.lit("customer").alias("source")))
    .distinct()
)
