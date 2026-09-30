from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-24").cast("date")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.initcap(F.trim("device")).alias("device_title"),
        F.to_date("started_at").alias("started_at_date"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    latest
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w3))

result = (
    islands
    .cube(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.when(F.col("session_id") >= 7, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.percentile_approx("session_id", 0.9).alias("session_id_p90"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
    )
)
