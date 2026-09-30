from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .orderBy(F.col("id").desc(), F.col("location").desc())
)
