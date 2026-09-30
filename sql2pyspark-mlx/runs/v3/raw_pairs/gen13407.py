from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "dept_id",
        "salary",
        F.row_number().over(w).alias("hire_date_row"),
        F.min("salary").over(w2).alias("dept_id_min_salary"),
    )
)
