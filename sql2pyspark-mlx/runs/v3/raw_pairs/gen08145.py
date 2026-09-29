from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.length("full_name") < 8)
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
    )
    .orderBy(F.col("job_title").desc(), F.col("id").desc())
)
