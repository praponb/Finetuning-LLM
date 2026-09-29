from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .select("id", "location", "budget", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "location", "budget")
    .orderBy(F.col("budget").desc())
)
