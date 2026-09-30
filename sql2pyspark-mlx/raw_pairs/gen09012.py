from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.month("started_at")).orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_part))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("last_known_country_2", F.last("country", ignorenulls=True).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "browser",
        "first_known_browser",
        "event_seq",
        "last_known_country_2",
        "row_seq",
        F.when(F.col("browser").rlike("^(Edge|Firefox)$"), "CORE").when(F.col("browser").rlike("^(Chrome|Safari)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("browser_zone"),
    )
)
