from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "HR", "Finance"]))
    .select(
        "id",
        "budget",
        F.sum("budget").over(w).alias("running_sum_budget"),
        F.count("budget").over(w).alias("running_count_budget"),
        F.min("budget").over(w).alias("running_min_budget"),
        F.lag("budget", 1, 0).over(w2).alias("prev_budget"),
    )
)
