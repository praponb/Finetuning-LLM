from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(~F.col("d.dept_name").isin(["Marketing", "Sales", "HR"]) & (F.col("d.location") == "London"))
    .select(F.col("d.location"), F.col("d.budget"), F.col("e.is_remote"))
)
