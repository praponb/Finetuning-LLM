from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Engineer"]))
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        "level",
        F.lead("salary", 1, 0).over(w).alias("next_salary"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
    .orderBy("level", "dept_id")
)
