from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter((F.col("hire_date") >= "2024-12-01") & F.col("is_remote"))
    .withColumn("hire_date_plus_months", F.add_months("hire_date", 3))
)

employees_stats = (
    recent_employees
    .groupBy(
        F.when(F.col("level").isin(["L2", "L3"]), "tier_1").otherwise("other").alias("level_group"),
    )
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    employees_stats
    .orderBy("level_group")
    .limit(5)
)
