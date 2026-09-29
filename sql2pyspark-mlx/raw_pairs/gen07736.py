from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1968000, 2983000))
    .select("id", "dept_name", "location")
    .orderBy("dept_name", "id")
    .limit(5)
)
