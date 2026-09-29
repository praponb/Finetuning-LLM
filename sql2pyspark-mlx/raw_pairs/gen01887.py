from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("budget").between(1642000, 2387000))
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("location_prefix", F.substring("location", 1, 3))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.hire_date").alias("employee_hire_date"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        F.col("c.hire_date").alias("employee_hire_date_2"),
        F.col("c.level").alias("employee_level"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") < F.col("s.employee_hire_date_2")), "left_anti")
)

totals = (
    kept
    .groupBy(F.date_trunc("MONTH", "employee_hire_date_2").alias("activity_month"))
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("employee_level") == "L3", 1)).alias("l3_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("employee_level") == "L5", 1)).alias("l5_rows"),
    )
)

result = (
    totals
    .select("activity_month", "l3_count", "location_list", "distinct_ids", "l5_rows")
    .orderBy(F.col("l3_count").desc_nulls_first(), "distinct_ids")
)
