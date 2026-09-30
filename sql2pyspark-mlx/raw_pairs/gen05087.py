from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 6300)
    .select("id", "name", "age")
)
