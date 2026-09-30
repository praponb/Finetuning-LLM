from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(6800, 11100) & (F.col("credit_limit") >= 7500) & F.col("credit_limit").between(4500, 11800))
    .groupBy("city", "country")
    .agg(F.countDistinct("age").alias("unique_citys"))
    .filter(F.col("unique_citys") < 3)
    .orderBy("city", F.col("country").desc())
    .limit(5)
)
