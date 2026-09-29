from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Engineering", "Finance"]))
    .select(
        "budget",
        "dept_name",
        "location",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal"),
    )
    .orderBy(F.col("budget").desc())
)
