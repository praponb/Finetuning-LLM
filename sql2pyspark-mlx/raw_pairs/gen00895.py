from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Bangkok")
    .select("id", "email")
    .unionByName(df_customers.filter(F.col("age") < 42).select("id", "email"))
    .distinct()
)
