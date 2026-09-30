from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Singapore"]))
    .groupBy("is_active")
    .agg(F.countDistinct("credit_limit").alias("unique_citys"))
    .orderBy(F.col("unique_citys").desc())
)
