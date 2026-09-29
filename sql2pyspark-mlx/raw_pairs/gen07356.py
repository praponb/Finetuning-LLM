from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-06-04"))) & F.col("segment").isNotNull())
    .withColumn("signup_date_plus_months", F.add_months("signup_date", 12))
)

result = (
    filtered
    .groupBy("city", "country")
    .pivot("segment", ["SMB", "Corporate", "Consumer"])
    .agg(F.max("credit_limit").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "city",
        "country",
        F.col("SMB_amount").alias("smb_amount"),
        F.col("SMB_n").alias("smb_n"),
        F.col("Corporate_amount").alias("corporate_amount"),
        F.col("Corporate_n").alias("corporate_n"),
        F.col("Consumer_amount").alias("consumer_amount"),
        F.col("Consumer_n").alias("consumer_n"),
    )
)
