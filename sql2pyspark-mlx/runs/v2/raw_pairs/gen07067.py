from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("age").between(62, 65) & ((F.lower(F.trim("email")) == "henry@startup.io") | F.col("segment").isin(["Consumer", "SMB"])))
)

summary = (
    valid_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"))
)

result = (
    summary
    .filter(F.col("th_count") >= 40)
    .select("signup_year", "th_count")
    .orderBy(F.col("signup_year").desc(), F.col("th_count").desc())
    .limit(50)
)
