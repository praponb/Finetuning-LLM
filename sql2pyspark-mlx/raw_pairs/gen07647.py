from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("budget") > 2031000)
    .select("id", "budget", F.sum("budget").over(w).alias("running_budget"))
    .orderBy(F.col("running_budget").desc())
)
