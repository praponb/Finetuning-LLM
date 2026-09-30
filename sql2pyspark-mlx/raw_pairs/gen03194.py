from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc())

result = (
    df_departments
    .select("id", "budget", F.percent_rank().over(w).alias("budget_pct_rank"))
)
