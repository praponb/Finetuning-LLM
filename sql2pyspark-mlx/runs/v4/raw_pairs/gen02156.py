from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
        "customer_id",
    )
    .agg(
        F.sum("island_id").alias("total_island_id"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
    )
    .orderBy(F.col("customer_id").desc())
)
