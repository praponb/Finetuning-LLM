from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("started_at").desc(), "id")

with_windows = (
    df_sessions
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
    .withColumn("last_known_browser_2", F.last("browser", ignorenulls=True).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

result = (
    with_windows
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL"))
    .cube("row_seq_tier", "customer_id")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.sum("row_seq").alias("total_row_seq"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
