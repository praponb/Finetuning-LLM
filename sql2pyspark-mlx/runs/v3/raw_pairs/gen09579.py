from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(
        "id",
        "device",
        "started_at",
        "browser",
        F.last("browser").over(w).alias("last_browser"),
    )
)
