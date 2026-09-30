from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("budget").between(1988000, 3042000))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.sum("budget").over(w).alias("dept_name_sum_budget"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
)
