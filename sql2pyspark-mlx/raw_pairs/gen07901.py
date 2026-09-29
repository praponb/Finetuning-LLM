from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "txn_time",
        "merchant",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.max(F.when(F.col("merchant") == "Amazon", F.col("island_id"))).alias("amazon_max_island_id"),
        F.percentile_approx("island_id", 0.5).alias("island_id_p50"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.max("txn_time").alias("ended_at"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("island_id")).otherwise(0)).alias("7_eleven_island_id"),
    )
    .filter(F.col("7_eleven_island_id") > 0)
    .orderBy(F.col("amazon_max_island_id").desc_nulls_first())
)
