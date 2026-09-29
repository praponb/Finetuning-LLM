from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.sum("budget").over(w).alias("running_budget"),
    )
    .orderBy(F.col("id").desc())
)
