from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") & F.col("dept_name").isin(["Engineering", "Legal", "Sales"]))
    .select("id", "budget")
)
