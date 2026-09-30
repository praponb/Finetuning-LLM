from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "country",
        "started_at",
        "browser",
        F.first("browser").over(w).alias("first_browser"),
    )
)
