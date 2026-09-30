from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "age", "email", "segment")
    .orderBy(F.col("id").desc())
)
