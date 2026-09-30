from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .withColumn("salary_rounded", F.round("salary", 0))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("OTHER")))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_dims = (
    ranked
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.email").alias("employee_email"),
        F.col("b.salary").alias("employee_salary"),
    )
)

cleaned = (
    with_dims
    .filter((F.col("hire_date") > F.lit("2023-11-28").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "resolved_job_title",
        "employee_email",
        F.floor("salary").alias("salary_whole"),
    )
)

kept_keys = (
    cleaned
    .select("id", "dept_id")
    .exceptAll(
        df_employees
        .filter(F.col("salary").between(133000, 140000))
        .select("id", "dept_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "hire_date",
        "is_remote",
        "level",
        "resolved_job_title",
        "employee_email",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

result = (
    base
    .select(
        "id",
        "full_name",
        "dept_id",
        "is_remote",
        "level",
        "resolved_job_title",
        "employee_email",
        "is_remote_label",
        "resolved_level",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
