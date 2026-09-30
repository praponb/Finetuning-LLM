from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .filter(F.col("stddev_credit_limit") >= 51)
    .orderBy(F.col("stddev_credit_limit").desc())
)
