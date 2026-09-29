from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy(F.year("hire_date"), "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("hire_date"), "job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.percent_rank().over(w).alias("salary_percent_rank"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
        F.ntile(5).over(w3).alias("salary_bucket"),
    )
)
