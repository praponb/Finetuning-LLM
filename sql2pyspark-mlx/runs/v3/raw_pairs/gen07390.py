from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(~F.col("device").isin(["mobile", "tablet", "desktop"]))
    .select(
        "id",
        "browser",
        "started_at",
        F.lead("started_at", 2).over(w).alias("next_started_at"),
    )
)
