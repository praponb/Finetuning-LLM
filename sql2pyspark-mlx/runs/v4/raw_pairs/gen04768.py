from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget", "id")
w4 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "*",
        F.ntile(10).over(w).alias("budget_bucket"),
        F.sum("budget").over(w2).alias("running_budget"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("budget", ignorenulls=True).over(w4).alias("last_known_budget"),
        F.ntile(3).over(w5).alias("budget_bucket_2"),
    )
)
