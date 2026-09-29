from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") > 149000), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "dept_name",
        "budget",
        F.when(F.col("dept_name").rlike("^(Engineering|Finance)$"), "CORE").when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "GROUP_B").otherwise("PRIORITY").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name_zone").desc_nulls_last(), "dept_name")
)
