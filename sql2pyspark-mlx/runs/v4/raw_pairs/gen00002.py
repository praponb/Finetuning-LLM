from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNull())
    .select("country", "name")
    .intersect(df_customers.filter(F.col("age") >= 35).select("country", "name"))
)
