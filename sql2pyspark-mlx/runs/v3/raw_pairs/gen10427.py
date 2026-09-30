from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("segment") != "SMB")
    .withColumn("clean_name", F.trim("name"))
)

summary = (
    filtered_customers
    .groupBy("is_active")
    .agg(
        F.sum("age").alias("total_age"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)

result = (
    summary
    .select("is_active", "total_credit_limit")
    .orderBy(F.col("total_credit_limit").desc())
)
