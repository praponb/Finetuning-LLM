from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lag("budget").over(w).alias("prev_budget"),
        F.lead("budget").over(w).alias("next_budget"),
        F.row_number().over(w).alias("row_seq"),
        F.lead("budget", 1).over(w).alias("next_budget_2"),
    )
)
