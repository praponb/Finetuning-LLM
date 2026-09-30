from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("started_at").desc())
)
