from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Chrome"]))
    .select("id", "country", "started_at", F.rank().over(w).alias("rnk"))
)
