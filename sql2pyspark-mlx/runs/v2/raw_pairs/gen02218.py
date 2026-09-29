from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1944000)
    .select("dept_name", "location")
)
