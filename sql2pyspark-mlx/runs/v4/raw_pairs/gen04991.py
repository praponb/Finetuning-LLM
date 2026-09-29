from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("FR%"))
    .select("full_name")
    .unionByName(df_employees.filter(F.col("is_remote")).select("full_name"))
)
