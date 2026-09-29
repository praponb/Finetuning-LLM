from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2193000)
    .select("id", "dept_name")
)
