from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy(F.month("delivered_date_period_2")).orderBy(F.col("delivered_date_period_2").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy(F.month("delivered_date_period_2"), "status")
w4 = Window.partitionBy("status").orderBy(F.col("delivered_date_period_2").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("status", "carrier").orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("status").orderBy(F.col("delivered_date_period_2").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("order_id")
w8 = Window.partitionBy("order_id").orderBy(F.col("delivered_date_period_2").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy(F.year("delivered_date"))
w10 = Window.partitionBy(F.year("delivered_date_period_2"))
w11 = Window.partitionBy("order_id", "channel").orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w12 = Window.partitionBy("channel").orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "*",
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
        F.date_trunc("week", "delivered_date").alias("delivered_date_period_2"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

kept_keys = (
    cleaned
    .select("id", "status")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") > "2025-07-01")
        .select("id", "status")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("cost_count_7d", F.count("cost").over(w))
    .withColumn("cost_avg_7d", F.avg("cost").over(w2))
    .withColumn("cost_max_7d", F.max("cost").over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("cost_max_30d", F.max("cost").over(w4))
)

metrics_shipments = (
    metrics
    .filter(F.col("cost_max_30d") > 52)
    .select(
        "id",
        "order_id",
        "delivered_date",
        "delivered_date_period",
        "delivered_date_period_2",
        "carrier_clean",
        "cost_avg_7d",
        "cost_max_30d",
        F.sum("cost_avg_7d").over(w).alias("cost_avg_7d_sum_7d"),
        F.sum("cost_count_7d").over(w5).alias("cost_count_7d_sum_90d"),
        F.count("cost_avg_7d").over(w6).alias("cost_avg_7d_count_7d"),
    )
)

with_windows = (
    metrics_shipments
    .select(
        "*",
        F.sum("cost_count_7d_sum_90d").over(w7).alias("order_id_sum_cost_count_7d_sum_90d"),
        F.max("cost_max_30d").over(w8).alias("cost_max_30d_max_30d"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.channel"), F.col("b.shipping_fee"), F.col("b.amount"))
)

enriched = (
    joined
    .select(
        "*",
        F.round((F.col("cost_count_7d_sum_90d") / F.sum("cost_count_7d_sum_90d").over(w9)), 4).alias("cost_count_7d_sum_90d_share"),
        F.sum("cost_avg_7d_count_7d").over(w10).alias("delivered_date_period_2_year_sum_cost_avg_7d_count_7d"),
        F.count("cost_count_7d_sum_90d").over(w11).alias("cost_count_7d_sum_90d_count_90d"),
        F.sum("cost_count_7d_sum_90d").over(w12).alias("cost_count_7d_sum_90d_sum_7d"),
        F.round((F.col("cost_avg_7d_count_7d") / F.sum("cost_avg_7d_count_7d").over(w7)), 4).alias("cost_avg_7d_count_7d_share"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("WEEK", "delivered_date_period_2").alias("week_start"))
    .agg(
        F.max(F.when(F.col("channel") == "store", F.col("cost_max_30d_max_30d"))).alias("store_max_cost_max_30d_max_30d"),
        F.round(F.avg("order_id_sum_cost_count_7d_sum_90d"), 2).alias("avg_order_id_sum_cost_count_7d_sum_90d"),
    )
)
