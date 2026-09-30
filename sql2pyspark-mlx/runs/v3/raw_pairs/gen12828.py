from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Berlin") & (F.col("budget") > 2078000) & ~F.col("dept_name").isin(["Engineering", "HR"]))
    .select("budget")
)
