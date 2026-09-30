from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "location")
    .distinct()
    .orderBy(F.col("location").desc(), "dept_name")
    .limit(50)
)
