from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 33)
    .orderBy("credit_limit", F.col("age").desc(), "id")
    .limit(3)
)
