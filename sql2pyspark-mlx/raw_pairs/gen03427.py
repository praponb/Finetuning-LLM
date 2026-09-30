from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .cube(
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
)
