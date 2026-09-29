from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Tokyo", "New York"]))
    .orderBy(F.col("dept_name").desc())
)
