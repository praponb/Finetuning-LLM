from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 1)
    .select("id", "browser", "started_at", "country")
)
