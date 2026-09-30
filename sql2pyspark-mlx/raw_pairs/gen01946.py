from pyspark.sql import functions as F

min_age = df_customers.agg(F.min("age")).first()[0]

result = (
    df_customers
    .filter(F.col("age") < min_age)
    .select("id", "age")
    .orderBy(F.col("id").desc(), F.col("age").desc())
)
