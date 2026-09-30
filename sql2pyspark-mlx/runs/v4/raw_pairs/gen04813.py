from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("location")

result = (
    df_departments
    .select(
        "*",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.sum("budget").over(w_hist).alias("location_sum_budget"),
    )
)
