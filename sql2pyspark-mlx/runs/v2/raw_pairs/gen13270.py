from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("id", "city", F.date_add("signup_date", 75).alias("signup_date_plus_75d"))
)
