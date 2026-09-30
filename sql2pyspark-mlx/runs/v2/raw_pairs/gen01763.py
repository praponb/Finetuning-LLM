from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .select("location")
    .subtract(df_departments.filter(F.col("budget") <= 2995000).select("location"))
)
