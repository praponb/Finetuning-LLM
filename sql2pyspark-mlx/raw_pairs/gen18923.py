from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "device").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
