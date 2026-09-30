from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 11200) & F.col("email").like("%@startup.io") & F.col("credit_limit").between(6500, 14200))
    .select("id", "city", F.col("is_active").cast("int").alias("is_active_flag"))
    .orderBy("id")
)
