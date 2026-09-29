from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("signup_date_label", F.date_format("signup_date", "yyyy-MM"))
)

summary = (
    valid_customers
    .groupBy("segment")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)

result = summary.orderBy(F.col("segment").desc())
