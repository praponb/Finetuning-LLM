from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        "dept_name",
        "location",
        F.ceil("budget").alias("budget_whole"),
    )
    .orderBy("budget", "dept_name")
)
