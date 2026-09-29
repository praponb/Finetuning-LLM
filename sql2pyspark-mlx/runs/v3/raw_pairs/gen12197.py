from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"))
    .filter(F.col("smb_count") >= 1)
    .orderBy("signup_cohort")
)
