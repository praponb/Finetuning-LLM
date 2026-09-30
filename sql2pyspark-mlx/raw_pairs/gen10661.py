from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Marketing", "Sales"]) & (F.col("dept_name") == "Finance"))
    .select(
        "id",
        "dept_name",
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
        F.floor("budget").alias("budget_whole"),
    )
    .orderBy(F.col("budget_whole").desc(), "id")
    .limit(50)
)
