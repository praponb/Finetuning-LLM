from pyspark.sql import functions as F

base = df_customers.filter(~F.col("is_active"))

agg = (
    base
    .groupBy("city")
    .agg(F.max("age").alias("max_age"))
)

result = (
    agg
    .filter(F.col("max_age") > 31)
    .select("city", "max_age")
    .orderBy("city", F.col("max_age").desc())
)
