from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["JP", "UK"]))
    .select("name")
    .unionByName(df_customers.filter(F.year("signup_date") >= 2025).select("name"))
    .distinct()
)
