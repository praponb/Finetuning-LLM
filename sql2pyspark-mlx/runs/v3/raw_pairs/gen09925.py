from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select("id", "browser", "started_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "browser", "started_at")
    .orderBy("started_at", F.col("id").desc())
)
