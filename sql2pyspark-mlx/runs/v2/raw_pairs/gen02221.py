from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age").between(41, 61) & F.col("country").isin(["DE", "UK"])) | (F.col("age") > 54))
    .select("email", "name", F.col("is_active").cast("int").alias("is_active_flag"))
    .orderBy("is_active_flag")
)
