from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .select(
        "id",
        "started_at",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("seq_no") >= 9, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        "seq_no",
    )
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.max("started_at").alias("ended_at"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
)
