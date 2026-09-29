from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_id")
w_running = Window.partitionBy("job_title").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "*",
        F.max("salary").over(w_grp).alias("dept_id_max_salary"),
        F.sum("salary").over(w_grp).alias("dept_id_sum_salary"),
        F.lead("salary", 1).over(w_running).alias("next_salary"),
    )
)
