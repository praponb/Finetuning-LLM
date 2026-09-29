from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "country", "started_at")
    .orderBy("country")
)
