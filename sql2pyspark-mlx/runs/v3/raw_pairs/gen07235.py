from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("hire_date") < "2025-09-06")

level_group_summary = (
    valid_employees
    .groupBy(
        F.when(F.col("level").isin(["L4", "L2"]), "core").otherwise("other").alias("level_group"),
    )
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = level_group_summary.orderBy("level_group")
