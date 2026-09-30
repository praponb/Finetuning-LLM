from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNull() & (F.col("location") == "Bangkok") & (F.col("location") == "Tokyo"))
    .select(
        "id",
        "dept_name",
        "budget",
        F.col("budget").cast("int").alias("budget_int"),
    )
    .orderBy("budget", F.col("dept_name").desc())
)
