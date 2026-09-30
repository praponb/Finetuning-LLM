from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select("id", "location", "budget", F.lag("budget", 2).over(w).alias("prev_budget"))
    .orderBy("location", "budget")
)
