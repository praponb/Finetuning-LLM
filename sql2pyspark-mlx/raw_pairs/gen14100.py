from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w_ordered = Window.partitionBy(F.month("started_at")).orderBy("started_at", "id")

scored = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w_ordered).alias("event_seq_3"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        "device",
    )
    .agg(
        F.countDistinct("browser").alias("distinct_browsers"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("event_seq_per_desktop"),
    )
)
