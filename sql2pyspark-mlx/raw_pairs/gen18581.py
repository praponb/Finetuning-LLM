from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

with_windows = (
    df_departments
    .select(
        "id",
        "budget",
        F.lead("budget").over(w_hist).alias("next_budget"),
        F.lag("budget").over(w_hist).alias("prev_budget"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

result = (
    with_windows
    .select("id", "prev_budget")
    .orderBy(F.col("id").desc_nulls_last())
)
