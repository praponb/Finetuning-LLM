from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        "browser",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("id").desc())
)
