from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Tokyo"))))
    .select("*", F.substring("location", 1, 4).alias("location_prefix"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"), F.col("c.hire_date"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        "s.employee_salary",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.level").alias("employee_level"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("employee_salary") >= 149000, "LARGE").otherwise("SMALL").alias("employee_salary_tier"),
        F.date_trunc("MONTH", "employee_hire_date").alias("activity_month"),
    )
    .agg(
        F.count_if(F.col("employee_level") == "L5").alias("l5_events"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("cnt").desc_nulls_last())
)
