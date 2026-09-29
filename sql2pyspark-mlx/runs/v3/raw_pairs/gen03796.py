from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "salary",
        F.count("*").over(w).alias("dept_id_count"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
)
