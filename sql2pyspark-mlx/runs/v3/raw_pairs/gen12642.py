from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.month("hire_date").isin([1, 2, 8]) & F.col("manager_id").isNull() & F.col("is_remote"))
)

summary = (
    valid_employees
    .groupBy(
        "level",
        F.when(F.col("level").isin(["L3", "L4"]), "priority").otherwise("other").alias("level_group"),
    )
    .agg(F.count("*").alias("employee_count"))
)

result = summary.select("level", "level_group", "employee_count")
