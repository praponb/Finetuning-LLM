from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Sales", "HR"]))
    .select("dept_name", "location")
    .distinct()
)
