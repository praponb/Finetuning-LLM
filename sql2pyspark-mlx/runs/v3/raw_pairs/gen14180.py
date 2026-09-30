from pyspark.sql import functions as F

avg_credit_limit = df_customers.agg(F.avg("credit_limit")).first()[0]

result = (
    df_customers
    .filter(F.col("credit_limit") > avg_credit_limit)
    .select("id", "credit_limit")
    .orderBy("credit_limit")
)
