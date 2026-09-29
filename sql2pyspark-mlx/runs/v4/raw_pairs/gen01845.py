from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select("id", "country", "started_at", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .orderBy("row_num", F.col("id").desc())
)
