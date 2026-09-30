from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.lead("budget").over(w_grp).alias("next_budget"),
        F.lag("budget").over(w_grp).alias("prev_budget"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
