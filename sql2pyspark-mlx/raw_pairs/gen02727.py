from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.level").alias("employee_level"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < "2023-09-01"), "left_semi")
)

matched_keys = (
    kept
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(F.col("budget") >= 3652000)
        .select("dept_name", "id")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("Unknown")))
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title") == "Director"), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "dept_name",
        "dept_name_filled",
        F.when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "EXTENDED").when(F.col("dept_name").rlike("^(Marketing|HR)$"), "GROUP_A").otherwise("GROUP_B").alias("dept_name_zone"),
    )
)
