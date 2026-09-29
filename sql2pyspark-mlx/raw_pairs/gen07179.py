from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location")
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        F.ntile(10).over(w).alias("budget_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
        F.sum("budget").over(w4).alias("running_budget"),
    )
)

result = (
    scored
    .select("id", "dept_name", "running_budget")
    .orderBy(F.col("id").desc_nulls_last())
)
