from pyspark.sql import functions as F

avg_age = df_customers.filter(F.col("country").isin(["JP", "US"])).agg(F.avg("age")).first()[0]

result = (
    df_customers
    .filter(F.col("age") > avg_age)
    .select("id", "age", "is_active")
)
