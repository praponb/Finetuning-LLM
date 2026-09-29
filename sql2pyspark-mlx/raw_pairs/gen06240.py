from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "budget",
        "dept_name",
        "location",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)
