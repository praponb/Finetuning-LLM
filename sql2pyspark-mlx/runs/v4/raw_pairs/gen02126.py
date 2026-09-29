from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title")
w5 = Window.partitionBy("dept_name").orderBy("running_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level")
w7 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("seq_no_share").desc_nulls_last(), "id")
w8 = Window.partitionBy("dept_name", "job_title").orderBy("seq_no_share", "id")
w9 = Window.partitionBy("dept_name")
w10 = Window.partitionBy("job_title").orderBy("salary", "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-25").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("salary").over(w).alias("running_salary"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.dept_name"))
)

with_windows = (
    enriched
    .withColumn("cumulative_running_salary", F.sum("running_salary").over(w3))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w4)), 4))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w5))
    .withColumn("level_avg_salary", F.avg("salary").over(w6))
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w4)), 4))
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    ranked
    .filter(F.col("level").isNotNull() & F.col("salary_share").between(0.48, 0.59))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "full_name_filled",
        "running_salary",
        "dept_name",
        "cumulative_running_salary",
        "salary_share",
        "last_known_job_title",
        "level_avg_salary",
        "seq_no_share",
        F.concat("dept_name", F.lit("/"), "email").alias("dept_name_email"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

metrics_employees = (
    valid
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("running_salary_share", F.round((F.col("running_salary") / F.sum("running_salary").over(w9)), 4))
    .withColumn("next_salary_share", F.lead("salary_share", 1).over(w10))
)

result = (
    metrics_employees
    .select(
        "id",
        "dept_id",
        F.col("salary_share").alias("SALARY_SHARE_VAL"),
        F.col("seq_no_share").alias("SEQ_NO_SHARE"),
        F.col("next_salary_share").alias("NEXT_SALARY_SHARE"),
    )
    .unpivot(
        ["id", "dept_id"],
        ["SALARY_SHARE_VAL", "SEQ_NO_SHARE", "NEXT_SALARY_SHARE"],
        "measure",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
