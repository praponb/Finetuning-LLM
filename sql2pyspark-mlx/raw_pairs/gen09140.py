from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level") == "L5"), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.when(F.col("dept_name").rlike("^(Finance|Engineering)$"), "PRIORITY").otherwise("GROUP_B").alias("dept_name_zone"),
    )
    .orderBy(F.col("budget").asc_nulls_first())
)
