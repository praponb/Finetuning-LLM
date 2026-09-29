from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "is_new_session",
        "island_id",
        F.when(F.col("browser").rlike("^(Firefox|Chrome)$"), "CORE").otherwise("PRIORITY").alias("browser_zone"),
    )
    .orderBy("island_id")
)
