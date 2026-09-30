from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Legal"]))
    .select(
        "id",
        "budget",
        "location",
        F.round((F.col("budget") * 1.15), 2).alias("budget_adjusted"),
    )
)
