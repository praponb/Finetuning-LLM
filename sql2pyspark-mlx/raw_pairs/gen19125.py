from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Finance", "HR"]))
    .select("location", "dept_name")
    .orderBy("location")
)
