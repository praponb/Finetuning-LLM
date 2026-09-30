from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

with_windows = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Marketing"]))
    .select(
        "id",
        "location",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.ntile(5).over(w3).alias("budget_bucket"),
    )
)

result = (
    with_windows
    .select("id", "location", "budget_share")
    .orderBy(F.col("budget_share").desc_nulls_last())
)
