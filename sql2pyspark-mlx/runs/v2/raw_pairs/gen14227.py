from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Ja%"))
    .select("email")
    .unionByName(df_employees.select("email"))
)
