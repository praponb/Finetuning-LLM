from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "dept_name", "budget")
    .orderBy(F.col("id").desc())
)
