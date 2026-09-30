from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "*",
        F.avg("salary").over(w_part).alias("salary_avg_6"),
        F.ntile(4).over(w2).alias("salary_bucket"),
    )
)
