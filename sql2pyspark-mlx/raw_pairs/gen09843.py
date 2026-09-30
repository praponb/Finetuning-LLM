from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("dept_name", "location").orderBy("budget")

enriched = (
    df_departments
    .select(
        "id",
        "location",
        F.row_number().over(w_part).alias("event_seq"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.percent_rank().over(w3).alias("budget_percent_rank"),
    )
)

result = (
    enriched
    .select("id", "location", "event_seq", "budget_percent_rank")
    .orderBy(F.col("budget_percent_rank").desc())
)
