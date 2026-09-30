from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.orderBy(F.col("budget").asc_nulls_first())
w3 = Window.orderBy(F.col("budget").asc_nulls_last())
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy(F.col("budget_percent_rank_2").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("dept_name").orderBy(F.col("salary").desc_nulls_first())

enriched = (
    df_departments
    .withColumn("budget_bucket", F.ntile(4).over(w))
    .withColumn("budget_percent_rank", F.percent_rank().over(w2))
    .withColumn("budget_percent_rank_2", F.percent_rank().over(w3))
    .withColumn("running_budget", F.sum("budget").over(w4))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_bucket",
        "budget_percent_rank",
        "running_budget",
    )
)

with_last = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        "s.budget_bucket",
        "s.budget_percent_rank",
        "s.running_budget",
        F.col("c.salary").alias("employee_salary"),
        F.col("c.job_title"),
    )
)

with_last_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.salary"))
)

valid = (
    with_last_departments
    .filter((F.col("dept_name").eqNullSafe(F.lit("Marketing"))) & F.col("dept_name").isNotNull())
    .select(
        "*",
        F.trim("dept_name").alias("clean_dept_name"),
        F.col("salary").cast("int").alias("salary_int"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "salary", "clean_dept_name")
)

daily = (
    latest
    .groupBy(
        F.when(F.col("salary") >= 130000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("salary")).otherwise(0)).alias("hr_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.avg("salary").alias("avg_salary_2"),
        F.countDistinct("dept_name").alias("distinct_dept_names_2"),
    )
)

result = (
    daily
    .select(
        "salary_tier",
        "distinct_dept_names",
        "salary_quartiles",
        "hr_salary",
        "avg_salary_2",
    )
    .orderBy("distinct_dept_names", F.col("hr_salary").desc())
)
