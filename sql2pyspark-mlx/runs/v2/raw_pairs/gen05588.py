from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .select("id", "dept_name", "location")
    .orderBy("dept_name", "id")
    .limit(1)
)
