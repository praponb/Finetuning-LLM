from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.l1_salary") > 63)
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ebd.l1_salary"),
        F.col("ebd.level_list"),
    )
    .orderBy("l1_salary", "dept_name")
)
