from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .select("location", "dept_name")
    .distinct()
    .orderBy(F.col("location").desc(), F.col("dept_name").desc())
    .limit(20)
)
