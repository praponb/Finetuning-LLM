from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())
w3 = Window.orderBy("budget", "id")

metrics = (
    df_departments
    .filter(~F.col("location").isin(["Tokyo", "Berlin"]))
    .select(
        "*",
        F.first("dept_name", ignorenulls=True).over(w).alias("first_known_dept_name"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
        F.lag("budget", 1).over(w3).alias("prev_budget"),
        F.last("dept_name", ignorenulls=True).over(w).alias("last_known_dept_name"),
        F.last("location", ignorenulls=True).over(w).alias("last_known_location"),
    )
)

result = (
    metrics
    .select(
        "id",
        "budget",
        "first_known_dept_name",
        "budget_percent_rank",
        "prev_budget",
        "last_known_dept_name",
        "last_known_location",
    )
    .orderBy("prev_budget")
)
