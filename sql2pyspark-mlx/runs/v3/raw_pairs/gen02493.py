from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select("id", "device", "started_at", "browser", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "device", "started_at", "browser")
)
