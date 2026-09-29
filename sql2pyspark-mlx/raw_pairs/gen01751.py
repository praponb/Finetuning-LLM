from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("shipped_date")).orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("shipped_date")).orderBy("shipped_date", "id")
w5 = Window.orderBy("row_seq")

cleaned = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
    )
)

filtered = (
    cleaned
    .filter(F.col("shipped_date").between("2023-11-01", "2024-08-01"))
    .withColumn("carrier_title", F.initcap(F.trim("carrier")))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("OTHER")))
)

with_windows = (
    filtered
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w2))
    .withColumn("first_known_carrier_2", F.first("carrier", ignorenulls=True).over(w3))
)

metrics = (
    with_windows
    .filter(F.col("order_id").isin([10, 11]))
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.lag("row_seq", 2).over(w).alias("prev_row_seq"),
        F.dense_rank().over(w5).alias("row_seq_dense_rank"),
    )
)

result = (
    metrics
    .rollup(
        F.when(F.col("row_seq_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("row_seq_dense_rank_tier"),
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.count_if(F.col("carrier") == "Flash").alias("flash_events"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
    .orderBy(F.col("row_seq_dense_rank_tier").desc_nulls_last())
)
