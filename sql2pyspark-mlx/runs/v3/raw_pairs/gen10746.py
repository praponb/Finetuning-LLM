from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(5400, 16000))
    .select("country")
    .distinct()
)
