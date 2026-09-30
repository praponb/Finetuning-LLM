from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("budget").desc(), "id")
w_part = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, -1)

result = (
    df_departments
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_2"),
        F.ntile(4).over(w2).alias("budget_bucket"),
        F.sum("budget").over(w_part).alias("running_budget"),
        F.avg("budget").over(w4).alias("budget_avg_5_prior"),
    )
)
