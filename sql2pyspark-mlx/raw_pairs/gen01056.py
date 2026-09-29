from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .filter(F.col("budget") <= 2437000)
    .select("id", "location", "budget", F.rank().over(w).alias("budget_rank"))
)
