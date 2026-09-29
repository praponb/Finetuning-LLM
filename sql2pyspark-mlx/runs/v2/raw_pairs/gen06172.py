from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.row_number().over(w).alias("budget_row"),
        (F.col("budget") - F.lag("budget", 2).over(w2)).alias("budget_change"),
    )
)
