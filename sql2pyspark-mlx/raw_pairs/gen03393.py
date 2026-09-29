from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "level",
        "dept_id",
        "hire_date",
        "salary",
        F.row_number().over(w).alias("hire_date_row"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
