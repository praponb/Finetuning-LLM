from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Edge", "Safari"]))
    .select("id", "device", "started_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .orderBy("started_at")
)
