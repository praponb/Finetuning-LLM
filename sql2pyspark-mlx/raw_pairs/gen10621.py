from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.sum("budget").over(w).alias("running_budget"),
    )
)
