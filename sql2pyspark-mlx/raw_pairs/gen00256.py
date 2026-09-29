from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("job_title").orderBy("salary")
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-5, -1)
w_hist = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "manager_id").orderBy(F.col("cumulative_salary").desc())

metrics = (
    df_employees
    .select(
        "*",
        F.rank().over(w_grp).alias("salary_rank"),
        F.avg("salary").over(w2).alias("salary_avg_5_prior"),
        F.sum("salary").over(w_hist).alias("cumulative_salary"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "salary_rank",
        "salary_avg_5_prior",
    )
)

with_dims = (
    latest
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.budget"), F.col("d.dept_name"))
)

base = (
    with_dims
    .filter(F.col("job_title").isNotNull() & F.col("email").like("%@startup.io"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary_rank",
        "salary_avg_5_prior",
        "budget",
        "dept_name",
        F.trim("dept_name").alias("clean_dept_name"),
    )
)

result = (
    base
    .groupBy("dept_id")
    .pivot("dept_name", ["Finance", "Engineering"])
    .agg(F.sum("salary_rank").alias("total"), F.count(F.lit(1)).alias("n"))
)
