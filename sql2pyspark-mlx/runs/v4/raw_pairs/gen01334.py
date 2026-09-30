from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 3066000) & (F.col("dept_name") == "HR"))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lower("dept_name").alias("dept_name_lower"),
        F.col("budget").cast("int").alias("budget_int"),
    )
)
