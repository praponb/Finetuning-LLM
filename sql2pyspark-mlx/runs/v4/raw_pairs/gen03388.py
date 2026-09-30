from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["JP", "DE"]))
    .select("name")
    .union(df_employees.select("full_name"))
    .distinct()
)
