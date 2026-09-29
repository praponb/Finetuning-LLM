from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") & (F.col("budget") < 3094000) & (F.col("budget") <= 2939000))
    .select("id", "dept_name")
)
