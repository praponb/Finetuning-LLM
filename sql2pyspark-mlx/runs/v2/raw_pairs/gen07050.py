from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "signup_date", F.col("is_active").cast("int").alias("is_active_flag"))
    .orderBy(F.col("signup_date").desc(), "id")
    .limit(1)
)
