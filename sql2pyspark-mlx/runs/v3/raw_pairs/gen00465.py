from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("country") == "UK")

agg = (
    valid_customers
    .groupBy("city")
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
)

result = (
    agg
    .select("city", "stddev_credit_limit")
    .orderBy("city")
    .limit(10)
)
