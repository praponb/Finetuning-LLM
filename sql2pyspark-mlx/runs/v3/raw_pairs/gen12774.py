from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "salary",
        "dept_id",
        F.lead("salary", 1).over(w).alias("next_salary"),
        F.lead("salary", 2).over(w2).alias("next_salary_2"),
    )
)
