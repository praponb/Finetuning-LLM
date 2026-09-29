from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("job_title", "dept_id").orderBy(F.col("hire_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-04-02"))) & F.col("manager_id").isNotNull())
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("clean_job_title", F.trim("job_title"))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "clean_job_title",
    )
)

valid_employees = (
    deduped
    .filter(F.lower(F.trim("email")) == "kanya@mail.co.th")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "clean_job_title",
        F.trim("full_name").alias("clean_full_name"),
    )
)

latest = (
    valid_employees
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "clean_job_title",
    )
)

result = (
    latest
    .select(
        "id",
        "full_name",
        "dept_id",
        "level",
        F.first("job_title", ignorenulls=True).over(w3).alias("first_known_job_title"),
    )
)
