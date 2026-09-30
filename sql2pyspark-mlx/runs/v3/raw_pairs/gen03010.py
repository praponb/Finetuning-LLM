from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Sales", "Engineering"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("department_count"),
    )
    .orderBy("dept_name_group")
)
