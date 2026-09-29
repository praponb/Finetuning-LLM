from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3009000, 3762000))
    .select("id", "location", "dept_name")
    .orderBy("dept_name")
)
