from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("age") >= 59)
    .withColumn("credit_limit_int", F.col("credit_limit").cast("int"))
)

result = (
    filtered
    .groupBy("segment")
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.avg("credit_limit_int").alias("avg_credit_limit_int"),
        F.avg(F.when(F.col("segment") == "SMB", F.col("age"))).alias("smb_avg_age"),
    )
    .orderBy("avg_age")
)
