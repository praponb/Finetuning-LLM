from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("island_id") >= 6, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.count("*").alias("row_count"),
        F.max("paid_at").alias("ended_at"),
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        F.max("is_new_session").alias("max_is_new_session"),
    )
    .filter(F.col("island_id_p90") < 0)
    .orderBy("row_count")
)
