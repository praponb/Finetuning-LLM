from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Legal", "Marketing"]) & (F.col("budget") >= 3793000) & F.col("dept_name").isNotNull())
    .select("location")
    .orderBy("location")
)
