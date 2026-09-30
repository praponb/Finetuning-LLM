from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit"))).alias("us_total_credit_limit"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("age_per_smb"),
    )
    .orderBy("corr_age_credit_limit")
)
