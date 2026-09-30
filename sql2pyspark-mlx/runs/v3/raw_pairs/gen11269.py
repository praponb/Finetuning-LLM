from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1793000)
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.col("budget").cast("int").alias("budget_int"),
    )
)
