from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.percent_rank().over(w).alias("budget_pct_rank"),
    )
)
