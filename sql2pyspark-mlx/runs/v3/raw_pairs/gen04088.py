from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Tokyo", "Singapore"]))
    .select("id", "location", "budget", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
)
