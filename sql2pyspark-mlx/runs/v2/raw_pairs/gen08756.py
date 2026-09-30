from pyspark.sql import functions as F

avg_credit_limit = df_customers.filter(F.col("credit_limit").between(10300, 12800)).agg(F.avg("credit_limit")).first()[0]

result = (
    df_customers
    .filter(F.col("credit_limit") >= avg_credit_limit)
    .select("id", "credit_limit", "is_active")
)
