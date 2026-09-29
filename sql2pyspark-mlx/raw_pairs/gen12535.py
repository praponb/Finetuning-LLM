from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-07-11").cast("date"))
    .select(
        "*",
        F.round((F.col("duration_sec") * 100), 2).alias("adjusted_duration_sec"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id_2", F.sum("is_new_session").over(w2))

ranked = (
    islands
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    ranked
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.device"), F.col("p.country"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
        F.round(F.sum("session_id_2") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("session_id_2_per_search"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_rows"),
    )
    .filter(F.col("checkout_duration_sec") < 1000000)
    .orderBy(F.col("checkout_duration_sec").desc_nulls_first())
)
