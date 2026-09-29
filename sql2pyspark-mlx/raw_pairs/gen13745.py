from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.row_number().over(w).alias("row_num"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)
