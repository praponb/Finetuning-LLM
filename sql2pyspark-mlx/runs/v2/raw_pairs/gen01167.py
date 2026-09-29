from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~F.col("m.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.level").alias("manager_level"),
        F.weekofyear(F.col("e.hire_date")).alias("hire_date_weekofyear"),
    )
    .orderBy("manager_level", F.col("hire_date_weekofyear").desc(), "id")
    .limit(20)
)
