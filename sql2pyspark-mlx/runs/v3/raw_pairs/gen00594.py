from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "dept_name")
    .orderBy("dept_name", F.col("id").desc())
    .limit(5)
)
