from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "is_new_burst",
        F.when(F.col("browser").rlike("^(Edge|Safari)$"), "CORE").when(F.col("browser").rlike("^(Chrome|Firefox)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("browser_zone"),
    )
    .orderBy(F.col("browser_zone").desc())
)
