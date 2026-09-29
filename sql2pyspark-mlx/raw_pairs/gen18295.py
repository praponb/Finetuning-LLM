from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("city").isin(["Tokyo", "Singapore"]), "group_a").otherwise("other").alias("city_group"),
        "is_active",
    )
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", F.col("age")).otherwise(0)).alias("smb_age"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("max_age") < 57)
    .drop("max_age")
)
