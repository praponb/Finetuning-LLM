from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.last("country").over(w).alias("last_country"),
    )
)
