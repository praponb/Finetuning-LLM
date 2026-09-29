from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget")

result = (
    df_departments
    .select("id", "budget", F.percent_rank().over(w).alias("budget_pct_rank"))
    .orderBy("id")
)
