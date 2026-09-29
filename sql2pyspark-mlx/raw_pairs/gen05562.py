from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") > 66000), "left_anti")
)

latest_match_departments = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.employee_level",
        F.col("c.level").alias("employee_level_2"),
    )
)

eligible = (
    latest_match_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id") == 1), "left_anti")
)

base = (
    eligible
    .filter(F.col("dept_name").isNotNull() & F.col("employee_level").isin(["L3", "L4"]))
    .select(
        "*",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("employee_level", F.lit("DIRECT")).alias("employee_level_filled"),
    )
)

result = base.select("id", "dept_name", "employee_level", "dept_name_lower")
