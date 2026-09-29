from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull() & F.col("name").isNotNull())
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)
