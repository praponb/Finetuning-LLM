from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Legal", "Finance"]) & F.col("dept_name").isin(["Sales", "Legal"]))
    .select("id", "dept_name", "budget")
)
