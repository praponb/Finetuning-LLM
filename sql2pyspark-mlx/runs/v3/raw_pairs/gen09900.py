from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "Corporate", "SMB"]))
    .groupBy(
        F.year("signup_date").alias("signup_year"),
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
    )
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
    .filter(F.col("stddev_age") >= 76)
    .orderBy("signup_month", "signup_year")
    .limit(10)
)
