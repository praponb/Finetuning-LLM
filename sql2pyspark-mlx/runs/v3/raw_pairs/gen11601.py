from pyspark.sql import functions as F

result = (
    df_employees
    .rollup(
        "dept_id",
        F.when(F.col("level").isin(["L4", "L5"]), "group_a").otherwise("other").alias("level_group"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.min("hire_date").alias("min_hire_date"),
    )
    .orderBy("level_group")
)
