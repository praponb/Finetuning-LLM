from pyspark.sql import functions as F

max_credit_limit = df_customers.agg(F.max("credit_limit")).first()[0]

result = (
    df_customers
    .filter(F.col("credit_limit") > max_credit_limit)
    .select("id", "credit_limit", "country", "name")
)
