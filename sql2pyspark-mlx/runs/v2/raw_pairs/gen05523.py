from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.col("salary") <= 60000)
    .select(
        "id",
        "hire_date",
        "job_title",
        "salary",
        F.ntile(4).over(w).alias("quartile"),
        F.percent_rank().over(w2).alias("salary_pct_rank"),
    )
)
