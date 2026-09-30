from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w_recent = Window.partitionBy("dept_name", "location").orderBy("budget_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("budget_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("budget").between(2334000, 3845000))
    .select("*", F.col("budget").cast("int").alias("budget_int"))
)

result = (
    valid
    .select(
        "id",
        "dept_name",
        "location",
        "budget_int",
        F.ntile(4).over(w).alias("budget_bucket"),
        F.last("location", ignorenulls=True).over(w_recent).alias("last_known_location"),
        F.sum("budget_int").over(w_hist).alias("running_budget_int"),
        F.avg("budget_int").over(w4).alias("budget_int_avg_5"),
    )
)
