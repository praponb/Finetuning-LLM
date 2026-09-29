from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.lower(F.trim("name")) == "olivia garcia") & F.col("name").like("%For%"))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "segment")
    .agg(
        F.max("age").alias("max_age"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("smb_credit_limit") < 55)
    .orderBy(F.col("min_credit_limit").desc(), F.col("max_age").desc())
)
