from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc())
w2 = Window.partitionBy("status").orderBy("dedup_rank", "id")
w3 = Window.partitionBy("carrier", "status").orderBy("dedup_rank", "id")
w4 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w6 = Window.partitionBy("order_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-03-16").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.concat_ws(",", "carrier", "status").alias("carrier_status_key"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.first("carrier", ignorenulls=True).over(w4).alias("first_known_carrier"),
        F.row_number().over(w5).alias("seq_no_2"),
        F.first("carrier", ignorenulls=True).over(w6).alias("first_known_carrier_2"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("seq_no_2") >= 7, "HIGH").otherwise("LOW").alias("seq_no_2_tier"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("seq_no_2")).otherwise(0)).alias("delivered_seq_no_2"),
    )
    .filter(F.col("delivered_seq_no_2") >= 0)
)
