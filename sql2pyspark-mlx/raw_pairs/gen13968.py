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
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.row_number().over(w2).alias("hire_date_row"),
    )
)
