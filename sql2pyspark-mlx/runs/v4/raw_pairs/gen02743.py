from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("created_at").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("created_at", "id", "key")
w3 = Window.partitionBy("key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("island_id", "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("island_id").desc())
w6 = Window.partitionBy("key")

flattened = df_carts.select("id", "created_at", F.explode("attributes").alias("key", "value"))

scored = (
    flattened
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("key", ignorenulls=True).over(w3).alias("first_known_key"),
    )
)

with_prev = scored.withColumn("prev_created_at", F.lag("created_at").over(w2))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_created_at").isNull() | (F.col("created_at") > F.col("prev_created_at") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

enriched = (
    sessionized
    .select(
        "*",
        F.ntile(4).over(w4).alias("island_id_bucket"),
        F.rank().over(w5).alias("island_id_rank"),
        F.min("session_start_flag").over(w6).alias("key_min_session_start_flag"),
    )
)

result = (
    enriched
    .select(
        "id",
        "created_at",
        "key",
        "row_seq",
        "first_known_key",
        "prev_created_at",
        "island_id",
        "island_id_bucket",
        "island_id_rank",
        F.when(F.col("key").rlike("^(device|utm_source)$"), "CORE").otherwise("PRIORITY").alias("key_zone"),
    )
)
