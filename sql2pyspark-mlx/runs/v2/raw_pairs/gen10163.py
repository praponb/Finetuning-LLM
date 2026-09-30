from pyspark.sql import functions as F

avg_age = df_customers.agg(F.avg("age")).first()[0]

result = (
    df_customers
    .filter(F.col("age") >= avg_age)
    .select("id", "age", "country")
    .orderBy(F.col("country").desc(), F.col("id").desc())
)
