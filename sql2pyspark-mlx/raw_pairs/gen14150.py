from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Chiang Mai", "Bangkok"]))
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.max("age").alias("max_age"),
    )
)
