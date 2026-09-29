from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("level").orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("employee_job_title").orderBy(F.col("employee_salary").desc_nulls_first(), "id")

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level"),
    )
)

latest = (
    with_last
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_last_departments = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.salary").alias("employee_salary"),
    )
)

ranked = (
    with_last_departments
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

stats = (
    ranked
    .groupBy("level")
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("employee_job_title") == "Director", 1)), F.lit(0)), 2).alias("budget_per_director"),
        F.sum(F.when(F.col("level") == "L5", F.col("budget")).otherwise(0)).alias("l5_budget"),
    )
)

result = (
    stats
    .select("level", "budget_per_director")
    .orderBy(F.col("budget_per_director").desc_nulls_last())
)
