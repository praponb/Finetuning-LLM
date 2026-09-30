from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isNull())
    .select("id", "budget", F.lag("budget", 1, 0).over(w).alias("prev_budget"))
)
