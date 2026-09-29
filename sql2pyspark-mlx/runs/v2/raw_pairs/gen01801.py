from pyspark.sql import functions as F

avg_budget = df_departments.filter(~F.col("dept_name").isin(["Legal", "Engineering", "Finance"])).agg(F.avg("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") > avg_budget)
    .select("id", "budget", "location", "dept_name")
)
