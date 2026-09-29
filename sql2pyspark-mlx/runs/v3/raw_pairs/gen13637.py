from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "mobile"]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "country", "started_at")
)
