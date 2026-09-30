from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "salary",
        "manager_id",
        F.lag("salary", 2, 0).over(w).alias("prev_salary"),
        F.rank().over(w2).alias("salary_rank"),
    )
)
