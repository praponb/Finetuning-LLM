from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .union(df_departments.select("location"))
    .orderBy("city")
)
