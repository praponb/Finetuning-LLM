from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("is_active") | (F.col("segment") == "SMB"))
    .withColumn("segment_prefix", F.substring("segment", 1, 4))
)

summary = (
    filtered_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
)

result = summary.filter(F.col("stddev_credit_limit") < 25)
