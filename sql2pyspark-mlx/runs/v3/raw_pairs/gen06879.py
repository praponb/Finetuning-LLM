from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 41)
    .orderBy(F.col("age").desc())
)
