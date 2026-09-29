from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("country") != "JP")
    .withColumn("segment_group", F.when(F.col("segment").isin(["Consumer", "SMB"]), "Core").otherwise("Other"))
)

signup_quarter_summary = (
    valid_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.max("signup_date").alias("last_signup_date"))
)

result = (
    signup_quarter_summary
    .select("signup_quarter", "last_signup_date")
    .orderBy(F.col("signup_quarter").desc())
)
