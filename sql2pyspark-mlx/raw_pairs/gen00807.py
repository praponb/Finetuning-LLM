from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("employee_job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title", "employee_job_title").orderBy(F.col("hire_date").desc(), "id")
w5 = Window.partitionBy("job_title").orderBy("hire_date", "id")

joined = (
    df_employees
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.hire_date",
        "a.is_remote",
        "a.level",
        F.col("e.email").alias("employee_email"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.full_name").alias("employee_full_name"),
    )
)

latest = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "full_name",
        "job_title",
        "hire_date",
        "is_remote",
        "employee_job_title",
        "employee_full_name",
    )
)

scored = (
    latest
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "job_title",
        "hire_date",
        "employee_job_title",
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
        F.first("employee_job_title", ignorenulls=True).over(w3).alias("first_known_employee_job_title"),
        F.last("employee_job_title", ignorenulls=True).over(w2).alias("last_known_employee_job_title"),
    )
)

scored_employees = (
    scored
    .select(
        "id",
        "full_name",
        "last_known_job_title",
        "first_known_employee_job_title",
        F.row_number().over(w4).alias("row_seq"),
        F.first("employee_job_title", ignorenulls=True).over(w2).alias("first_known_employee_job_title_2"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

report = (
    scored_employees
    .select(
        "id",
        "full_name",
        "last_known_job_title",
        "first_known_employee_job_title",
        "first_known_employee_job_title_2",
        F.when(F.col("full_name").rlike("^(Jack Taylor|David Lee )$"), "GROUP_B").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("full_name_zone"),
    )
    .orderBy(F.col("id").desc(), "full_name_zone")
)

result = (
    report
    .select(
        "id",
        "full_name",
        "last_known_job_title",
        "first_known_employee_job_title",
        "first_known_employee_job_title_2",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
