from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy("budget", "id")
w_ordered = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.lead("budget").over(w_running).alias("next_budget"),
        F.lag("budget").over(w_running).alias("prev_budget"),
        F.row_number().over(w_running).alias("seq_no"),
        F.lag("budget", 2).over(w_ordered).alias("prev_budget_2"),
    )
)
