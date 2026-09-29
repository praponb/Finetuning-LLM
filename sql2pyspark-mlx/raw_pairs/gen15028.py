from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w3 = Window.orderBy("budget", "id")
w_hist = Window.orderBy("budget")
w5 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .select(
        "*",
        F.last("budget", ignorenulls=True).over(w_running).alias("last_known_budget"),
        F.ntile(5).over(w2).alias("budget_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.percent_rank().over(w_hist).alias("budget_percent_rank"),
        F.first("dept_name", ignorenulls=True).over(w5).alias("first_known_dept_name"),
    )
)
