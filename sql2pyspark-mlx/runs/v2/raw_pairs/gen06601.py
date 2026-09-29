from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")
w2 = Window.orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.avg("budget").over(w).alias("dept_name_avg_budget"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
    .orderBy("prev_budget")
)
