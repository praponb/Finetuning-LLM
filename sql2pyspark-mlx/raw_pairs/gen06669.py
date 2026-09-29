from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2025-06-01")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "device", "started_at")
)
