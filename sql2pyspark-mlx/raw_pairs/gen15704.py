from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select(
        "id",
        "full_name",
        "email",
        "is_remote",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.lag("salary", 1).over(w2).alias("prev_salary"),
    )
)
