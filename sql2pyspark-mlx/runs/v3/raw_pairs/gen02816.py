from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNull())
    .select("id", "age", "name", "is_active")
    .orderBy(F.col("name").desc(), F.col("id").desc())
)
