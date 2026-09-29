from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select("id", "browser", "started_at", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("started_at").desc())
)
