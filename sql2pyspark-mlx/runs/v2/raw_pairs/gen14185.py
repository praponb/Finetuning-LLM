from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "hire_date",
        "salary",
        (F.col("salary") - F.lag("salary", 1).over(w)).alias("salary_change"),
    )
    .orderBy(F.col("salary_change").desc())
)
