from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.countDistinct("credit_limit").alias("unique_citys"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .filter(F.col("unique_citys") >= 4)
)
