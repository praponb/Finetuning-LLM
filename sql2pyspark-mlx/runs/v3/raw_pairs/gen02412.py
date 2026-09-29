from pyspark.sql import functions as F

result = (
    df_customers
    .rollup("segment", "is_active")
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
)
