from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("credit_limit") <= 4800))
    .select("id", "is_active")
    .orderBy(F.col("id").desc())
)
