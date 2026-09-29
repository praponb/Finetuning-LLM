from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
        F.first("country", ignorenulls=True).over(w_grp).alias("first_known_country"),
    )
)
