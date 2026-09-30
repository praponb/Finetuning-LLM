from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
    )
)
