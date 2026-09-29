from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("country") == "SG")
    .select("id", "device", "started_at", F.row_number().over(w).alias("row_num"))
)
