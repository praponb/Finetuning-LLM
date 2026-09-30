from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Legal", "HR"]))
    .select(
        "budget",
        "dept_name",
        "location",
        F.lower("dept_name").alias("dept_name_lower"),
    )
)
