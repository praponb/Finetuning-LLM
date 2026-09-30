from pyspark.sql import functions as F

min_credit_limit = df_customers.agg(F.min("credit_limit")).first()[0]

result = (
    df_customers
    .filter(F.col("credit_limit") < min_credit_limit)
    .select("id", "credit_limit", "segment", "name")
    .orderBy(F.col("credit_limit").desc(), "id")
    .limit(50)
)
