from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        "level",
        (F.col("salary") - F.lag("salary", 2).over(w)).alias("salary_change"),
        F.row_number().over(w2).alias("rn"),
    )
)
