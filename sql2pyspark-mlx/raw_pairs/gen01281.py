from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3650000)
    .select("dept_name")
    .subtract(df_suppliers.filter(F.col("country") == "US").select("country"))
)
