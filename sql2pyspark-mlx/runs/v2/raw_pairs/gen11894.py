from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Sales"]) & F.col("dept_name").isin(["Finance", "HR"]))
    .select("id", "location")
)
