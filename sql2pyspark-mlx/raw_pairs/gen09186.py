from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")

enriched = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "hire_date",
        "is_remote",
        F.lead("salary").over(w).alias("next_salary"),
        F.lag("salary").over(w).alias("prev_salary"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = enriched.select("id", "dept_id", "hire_date", "next_salary", "event_seq")
