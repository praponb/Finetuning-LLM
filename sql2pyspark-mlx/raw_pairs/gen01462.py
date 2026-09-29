from pyspark.sql import functions as F

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNull()), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "dept_name",
        "location",
        F.when(F.col("dept_name").rlike("^(Marketing|Finance)$"), "PRIORITY").otherwise("EXTENDED").alias("dept_name_zone"),
    )
    .orderBy("dept_name")
)
