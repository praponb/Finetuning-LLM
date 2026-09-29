from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") != "Consumer") & F.col("email").like("%@outlook.com"))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("name_filled", F.coalesce("name", F.lit("Unknown")))
    .orderBy(F.col("is_active_flag").desc())
)
