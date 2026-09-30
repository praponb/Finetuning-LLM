from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "US")
    .select("email")
    .subtract(df_employees.filter(F.col("full_name").like("FR%")).select("email"))
)
