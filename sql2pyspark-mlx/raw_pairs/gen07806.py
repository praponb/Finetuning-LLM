from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.first("budget", ignorenulls=True).over(w).alias("first_known_budget"),
        F.avg("budget").over(w2).alias("budget_avg_2"),
    )
)
