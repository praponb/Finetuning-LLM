from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id")
w4 = Window.partitionBy("browser", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_sessions
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w_running).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country"),
        F.first("browser", ignorenulls=True).over(w_recent).alias("first_known_browser"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("seq_no") >= 7, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.max(F.when(F.col("device") == "mobile", F.col("seq_no"))).alias("mobile_max_seq_no"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.min("started_at").alias("first_started_at"),
        F.max("started_at").alias("last_seen"),
    )
    .filter(F.col("edge_count") < 1)
    .orderBy(F.col("avg_seq_no").desc_nulls_first())
)
