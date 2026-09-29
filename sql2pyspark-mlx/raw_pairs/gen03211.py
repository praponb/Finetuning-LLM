from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "country")
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
    .filter(F.col("stddev_credit_limit") >= 37)
)
