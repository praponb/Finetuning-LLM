from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("job_title", "level").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")
w3 = Window.partitionBy("manager_id").orderBy(F.col("employee_hire_date").desc_nulls_first(), "id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("next_salary").desc_nulls_last())

scored = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.lead("salary", 1).over(w_recent).alias("next_salary"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.full_name",
        "src.manager_id",
        "src.salary",
        "src.hire_date",
        "src.next_salary",
        F.col("d.email"),
        F.col("d.hire_date").alias("employee_hire_date"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "next_salary",
        "email",
        "rn",
        "row_num",
    )
    .orderBy(F.col("next_salary").desc_nulls_last())
)
