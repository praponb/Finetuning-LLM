from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Sales")
    .select("location")
    .distinct()
)
