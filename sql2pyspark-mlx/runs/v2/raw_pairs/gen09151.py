from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Engineering", "HR"]) & (F.col("budget") > 2745000) & ~F.col("dept_name").isin(["Legal", "Sales"]))
    .select("dept_name")
)
