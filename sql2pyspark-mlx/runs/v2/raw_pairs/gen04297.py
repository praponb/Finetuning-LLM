from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "age")
    .orderBy(F.col("age").desc())
)
