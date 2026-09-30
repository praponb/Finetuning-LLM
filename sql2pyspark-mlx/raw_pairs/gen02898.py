from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("dept_name").orderBy(F.col("employee_salary").desc(), "id")

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("%Oliv%")), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        F.col("c.salary").alias("employee_salary"),
    )
)

eligible_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isin([12, 14, 40])), "left_anti")
)

deduped = (
    eligible_departments
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_semi")
)

daily = (
    kept
    .groupBy(
        F.when(F.col("employee_salary") >= 148000, "LARGE").otherwise("SMALL").alias("employee_salary_tier"),
        "dept_name",
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count("*").alias("event_count"),
        F.round(F.avg("employee_salary"), 2).alias("avg_employee_salary"),
        F.percentile_approx("employee_salary", 0.5).alias("employee_salary_p50"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
    )
    .filter(F.col("event_count") < 1000)
)

result = (
    daily
    .select(
        "employee_salary_tier",
        "dept_name",
        "event_count",
        "avg_employee_salary",
        "employee_salary_p50",
        "chiang_mai_rows",
    )
    .orderBy(F.col("chiang_mai_rows").asc_nulls_first())
)
