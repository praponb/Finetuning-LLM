from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc())
w2 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("seq_no").asc_nulls_first(), "id")

latest = (
    df_shipments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "carrier", "delivered_date", "status")
)

metrics = (
    latest
    .select(
        "id",
        "order_id",
        "status",
        F.row_number().over(w2).alias("seq_no"),
        F.first("carrier", ignorenulls=True).over(w3).alias("first_known_carrier"),
        F.last("carrier", ignorenulls=True).over(w4).alias("last_known_carrier"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped
    .filter(F.col("order_id").isin([13, 33, 40]))
    .select(
        "id",
        "order_id",
        "status",
        "first_known_carrier",
        "last_known_carrier",
        "rank_in_group",
        F.round((F.col("rank_in_group") / F.col("seq_no")), 2).alias("rank_in_group_per_seq_no"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("order_status"),
        F.col("src.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.rank_in_group_per_seq_no")).alias("min_rank_in_group_per_seq_no"),
    )
    .orderBy(F.col("cnt").desc())
)
