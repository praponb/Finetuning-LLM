from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.countDistinct("credit_limit").alias("unique_countrys"))
    .filter(F.col("unique_countrys") < 1)
    .orderBy("unique_countrys")
)
