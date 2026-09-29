from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")
w4 = Window.orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.shipped_date",
        "e.delivered_date",
        "e.cost",
        "e.status",
        F.col("b.channel"),
        F.col("b.order_date"),
    )
)

enriched_shipments = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.delivered_date",
        "e.cost",
        "e.channel",
        F.col("b.discount"),
    )
)

metrics = (
    enriched_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "channel",
        "discount",
        F.ntile(3).over(w).alias("cost_bucket"),
        F.avg("discount").over(w2).alias("order_id_avg_discount"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "shipped_date", "cost_bucket")
)

cleaned = (
    ranked
    .filter(F.col("shipped_date") > "2024-09-01")
    .withColumn("cost_bucket_band", F.when(F.col("cost_bucket") >= 7, "High").when(F.col("cost_bucket") >= 4, "Medium").otherwise("Low"))
    .withColumn("shipped_date_period", F.date_trunc("week", "shipped_date"))
)

metrics_shipments = (
    cleaned
    .select(
        "id",
        "shipped_date",
        F.avg("cost_bucket").over(w4).alias("cost_bucket_avg_30d"),
        F.max("cost_bucket").over(w5).alias("cost_bucket_max_7d"),
        F.avg("cost_bucket").over(w6).alias("cost_bucket_avg_3"),
    )
)

result = (
    metrics_shipments
    .withColumn("cost_bucket_max_7d_tier", F.when(F.col("cost_bucket_max_7d") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "shipped_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "shipped_date"))
    .groupingSets(
        [["cost_bucket_max_7d_tier", "week_start", "year_start"], ["cost_bucket_max_7d_tier", "week_start"], []],
        "cost_bucket_max_7d_tier",
        "week_start",
        "year_start",
    )
    .agg(
        F.percentile_approx("cost_bucket_avg_3", [0.25, 0.5, 0.75]).alias("cost_bucket_avg_3_quartiles"),
        F.percentile_approx("cost_bucket_avg_3", 0.5).alias("cost_bucket_avg_3_p50"),
    )
)
