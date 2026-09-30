from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("level").isin(["L1", "L5"]))
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        "dept_id",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.row_number().over(w2).alias("hire_date_row"),
    )
)
