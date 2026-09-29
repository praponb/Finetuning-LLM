from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2024-02-01")
    .select("id", "device", "started_at", F.row_number().over(w).alias("rn"))
    .orderBy("started_at")
)
