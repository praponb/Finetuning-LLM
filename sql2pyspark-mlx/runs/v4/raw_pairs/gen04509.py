from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.full_name").like("%Kany%") & (F.col("m.manager_id") == 5))
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("m.email").alias("manager_email"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.when(F.col("m.level").isin(["L2", "L4"]), "Tier 1").otherwise("Other").alias("level_group"),
    )
    .orderBy("level_group")
)
