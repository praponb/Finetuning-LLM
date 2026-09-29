from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai", "New York"]) | F.col("dept_name").isin(["Engineering", "Finance"]))
    .select("id", "dept_name", "budget")
    .orderBy(F.col("id").desc())
    .limit(10)
)
