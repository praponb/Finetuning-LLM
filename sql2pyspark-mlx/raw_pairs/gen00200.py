from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w4 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("is_new_burst", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy(F.col("is_new_burst_avg_6").asc_nulls_first())
w7 = Window.orderBy(F.col("last_known_is_new_burst").desc_nulls_first())
w8 = Window.orderBy(F.col("is_new_burst_avg_6_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("last_known_is_new_burst").desc_nulls_last())
w10 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w11 = Window.orderBy(F.col("is_new_burst_avg_6").asc_nulls_last())

flagged = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

latest = (
    sessionized
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "reading_ts", "is_new_burst")
)

scored = (
    latest
    .select(
        "id",
        "reading_ts",
        F.last("is_new_burst", ignorenulls=True).over(w4).alias("last_known_is_new_burst"),
        F.avg("is_new_burst").over(w5).alias("is_new_burst_avg_6"),
    )
)

kept_keys = (
    scored
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.hour("reading_ts").between(8, 15))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = kept_rows.withColumn("is_new_burst_avg_6_rank", F.rank().over(w6))

result = (
    enriched
    .select(
        "last_known_is_new_burst",
        "is_new_burst_avg_6",
        "is_new_burst_avg_6_rank",
        F.percent_rank().over(w7).alias("last_known_is_new_burst_percent_rank"),
        F.cume_dist().over(w8).alias("is_new_burst_avg_6_rank_cume_dist"),
        F.dense_rank().over(w9).alias("last_known_is_new_burst_dense_rank"),
        F.avg("is_new_burst_avg_6_rank").over(w10).alias("is_new_burst_avg_6_rank_avg_30m"),
        F.rank().over(w11).alias("is_new_burst_avg_6_rank_2"),
    )
)
