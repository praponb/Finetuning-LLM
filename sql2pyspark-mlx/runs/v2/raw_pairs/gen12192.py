from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "job_title",
        F.last("job_title").over(w).alias("last_job_title"),
    )
)
