from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 3763000) & (F.col("budget") >= 2963000))
    .select("id", "dept_name")
    .orderBy("id")
)
