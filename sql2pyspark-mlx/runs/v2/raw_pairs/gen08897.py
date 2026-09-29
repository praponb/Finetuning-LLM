from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Engineering")
    .select("id", "budget")
    .orderBy(F.col("budget").desc(), "id")
)
