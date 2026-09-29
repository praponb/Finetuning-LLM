from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy("delivered_date_period", "id")
w2 = Window.partitionBy("order_status").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("order_status").orderBy("cost", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("carrier", "order_id").orderBy("amount", "id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-06-18").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("delivered_date_period", F.date_trunc("MONTH", "delivered_date"))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.status").alias("order_status"), F.col("d.amount"))
)

valid = (
    with_dims
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-12-16"))) & F.col("order_id").isin([8, 28, 32, 35]))
    .withColumn("shipped_date_quarter", F.quarter("shipped_date"))
    .withColumn("amount_rounded", F.round("amount", 1))
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_shipments = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessions_shipments
    .withColumn("island_id_avg_3", F.avg("island_id").over(w3))
    .withColumn("island_id_avg_2_prior", F.avg("island_id").over(w4))
    .withColumn("amount_bucket", F.ntile(5).over(w5))
)

result = (
    metrics
    .groupBy(
        "order_id",
        F.when(F.col("amount") >= 1560, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("delivered_date_period")) - F.unix_timestamp(F.min("delivered_date_period"))) / 60).alias("duration_min"),
        F.min("delivered_date_period").alias("first_delivered_date_period"),
        ((F.unix_timestamp(F.max("delivered_date_period")) - F.unix_timestamp(F.min("delivered_date_period"))) / 60).alias("duration_min_2"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
    )
    .orderBy(F.col("duration_min_2").asc_nulls_first())
)
