from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(F.col("location").isin(["London", "Singapore"]) | (F.col("location") == "Singapore"))
)

dept_name_group_summary = (
    recent_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Marketing", "Finance"]), "priority").otherwise("other").alias("dept_name_group"),
        "dept_name",
    )
    .agg(F.sum("budget").alias("total_budget"))
)

result = dept_name_group_summary.orderBy("dept_name")
