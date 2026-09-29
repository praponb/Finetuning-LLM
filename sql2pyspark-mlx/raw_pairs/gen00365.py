from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("carrier_length").desc())
w_recent = Window.partitionBy(F.month("shipped_date"), "status").orderBy("shipped_date", "id")
w_grp = Window.partitionBy(F.date_trunc("MONTH", "shipped_date"), "order_id").orderBy("dedup_rank", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("carrier_length").desc_nulls_last())

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-03-22").cast("date")) & (F.col("shipped_date") > "2024-10-01"))
    .withColumn("carrier_length", F.length("carrier"))
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM-dd"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    ranked
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.store_id") == 7)
    .select("a.*", F.col("p.status").alias("order_status"))
)

scored = (
    with_dims
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("next_carrier_length", F.lead("carrier_length", 1).over(w_grp))
    .withColumn("carrier_length_dense_rank", F.dense_rank().over(w4))
)

result = (
    scored
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("activity_month"))
    .agg(
        F.round(F.avg("carrier_length_dense_rank"), 2).alias("avg_carrier_length_dense_rank"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
        F.count_if(F.col("carrier") == "Flash").alias("flash_events"),
        F.count_if(F.col("order_status") == "RETURNED").alias("returned_events"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("carrier_length")).otherwise(0)).alias("fedex_carrier_length"),
        F.count_if(F.col("order_status") == "CANCELLED").alias("cancelled_events"),
    )
    .orderBy("fedex_carrier_length")
)
