from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("No%") | (F.col("credit_limit") > 7100))
    .select("id", "email", "credit_limit")
)
