from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower(F.trim("name")) == "ivy chen")
    .select("name")
    .union(df_employees.select("full_name"))
    .orderBy("name")
)
