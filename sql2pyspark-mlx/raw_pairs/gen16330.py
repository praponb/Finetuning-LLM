from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("dept_name") != "Finance")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.sum("budget").over(w).alias("running_budget"),
    )
)
