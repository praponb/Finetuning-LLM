from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Sales"]))
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.location", "s.budget_int", F.col("c.level"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.job_title"))
)

with_last_departments = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.salary"))
)

result = (
    with_last_departments
    .groupBy(
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
        F.when(F.col("budget_int") >= 87, "HIGH").otherwise("LOW").alias("budget_int_tier"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("budget_int") / F.nullif(F.count(F.when(F.col("level") == "L4", 1)), F.lit(0)), 2).alias("budget_int_per_l4"),
        F.round(F.sum("budget_int") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("budget_int_per_l1"),
        F.countDistinct("employee_level").alias("distinct_employee_levels"),
    )
)
