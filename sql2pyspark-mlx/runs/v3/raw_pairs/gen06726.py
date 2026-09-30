from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Legal", "Sales"]) & F.col("budget").between(1437000, 2569000) & (F.col("dept_name") == "Finance"))
    .select("id", "location")
    .orderBy(F.col("location").desc(), "id")
    .limit(10)
)
