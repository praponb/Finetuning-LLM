from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Tokyo"]))
    .select("id", "budget", F.row_number().over(w).alias("budget_row"))
    .orderBy(F.col("id").desc(), F.col("budget").desc())
)
