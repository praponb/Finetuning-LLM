from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "Consumer") & ((F.col("country") == "SG") | (F.col("segment") == "SMB")))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
    .orderBy(F.col("signup_month").desc())
)
