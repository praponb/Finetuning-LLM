from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 3800000) & (F.col("dept_name") == "Finance"))
    .select("dept_name", "budget")
)
