from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy(F.year("hire_date"), "dept_id").orderBy("salary")
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.percent_rank().over(w2).alias("salary_percent_rank"),
        F.last("job_title", ignorenulls=True).over(w3).alias("last_known_job_title"),
    )
)
