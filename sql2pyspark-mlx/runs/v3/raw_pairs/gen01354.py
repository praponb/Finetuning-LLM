from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com"))
    .select("id", "credit_limit")
    .orderBy("id")
    .limit(1)
)
