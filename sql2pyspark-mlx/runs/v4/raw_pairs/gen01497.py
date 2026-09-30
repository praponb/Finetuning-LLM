from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id")

scored = (
    df_employees
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.min("salary").over(w2).alias("manager_id_min_salary"),
    )
)

result = (
    scored
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
        F.when(F.col("job_title").rlike("^(Manager|Engineer)$"), "GROUP_B").when(F.col("job_title").rlike("^(Analyst|Director)$"), "PRIORITY").otherwise("EXTENDED").alias("job_title_zone"),
    )
)
