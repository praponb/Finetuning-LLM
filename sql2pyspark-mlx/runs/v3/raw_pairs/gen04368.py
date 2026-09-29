from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lag("budget", 1).over(w).alias("prev_budget"),
    )
)
