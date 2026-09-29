from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2848000)
    .select("id", "location", "dept_name", "budget")
    .orderBy("dept_name", "id")
    .limit(20)
)
