from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Finance", "Engineering"]) & F.col("budget").between(1664000, 3819000))
    .select(
        "location",
        "dept_name",
        "budget",
        F.lower("location").alias("location_lower"),
        F.col("budget").cast("int").alias("budget_int"),
    )
)
