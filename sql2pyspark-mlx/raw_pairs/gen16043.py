from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("months_since_started_at").desc(), "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("country").eqNullSafe(F.lit("DE"))))
    .withColumn("months_since_started_at", (F.datediff(F.current_date(), "started_at") / 30))
    .withColumn("started_at_day", F.col("started_at").cast("date"))
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "months_since_started_at",
        "started_at_day",
        F.ntile(4).over(w).alias("months_since_started_at_bucket"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
    )
)
