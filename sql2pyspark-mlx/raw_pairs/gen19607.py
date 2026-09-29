from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("city", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.countDistinct("age").alias("unique_citys"))
    .filter(F.col("unique_citys") > 8)
    .select("city", "signup_month", "unique_citys")
    .orderBy("city", "signup_month")
    .limit(5)
)
