from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "browser",
        "country",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
    )
)
