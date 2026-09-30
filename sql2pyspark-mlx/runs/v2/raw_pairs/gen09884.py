from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "Consumer", "SMB"]) & F.col("signup_date").between("2025-01-03", "2025-04-01"))
    .select("id", "city", "name", F.floor("age").alias("age_whole"))
)
