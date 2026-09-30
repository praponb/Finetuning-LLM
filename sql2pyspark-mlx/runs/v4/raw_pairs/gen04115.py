from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "is_new_burst_tier", "burst_id_tier"], ["quarter_start", "is_new_burst_tier"]],
        "quarter_start",
        "is_new_burst_tier",
        "burst_id_tier",
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("is_new_burst_per_chrome"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
