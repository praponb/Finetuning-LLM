from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "New York") & F.col("dept_name").isin(["Finance", "Sales"]))
    .select("id", "dept_name")
    .orderBy("dept_name")
)
