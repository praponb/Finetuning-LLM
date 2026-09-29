from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull() & (F.col("dept_name") != "Sales"))
    .orderBy(F.col("id").desc(), "dept_name")
    .limit(10)
)
