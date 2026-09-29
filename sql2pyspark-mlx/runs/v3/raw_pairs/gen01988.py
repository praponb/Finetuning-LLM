from pyspark.sql import functions as F

recent_employees = df_employees.filter((F.col("manager_id") == 7) | F.col("dept_id").isin([18, 34]))

summary = (
    recent_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "is_remote",
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"))
)

result = (
    summary
    .select("hire_cohort", "level_values")
    .orderBy(F.col("level_values").desc(), "hire_cohort")
)
