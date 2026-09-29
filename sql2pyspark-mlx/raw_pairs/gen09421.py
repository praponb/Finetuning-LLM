from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .filter(F.col("budget").between(1311000, 2233000))
    .select("id", "location", "budget", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "location", "budget")
    .orderBy(F.col("budget").desc())
)
