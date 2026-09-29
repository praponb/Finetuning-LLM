from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") & F.col("budget").between(2412000, 3920000) & F.col("location").isin(["London", "New York", "Berlin"]))
    .select("id", "dept_name", "budget")
    .orderBy(F.col("id").desc())
)
