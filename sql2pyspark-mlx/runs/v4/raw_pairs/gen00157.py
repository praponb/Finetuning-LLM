from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-12-03").cast("date")) & F.col("manager_id").isNotNull())
    .select("*", F.coalesce("email", F.lit("Unknown")).alias("email_filled"))
)

result = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
    )
)
