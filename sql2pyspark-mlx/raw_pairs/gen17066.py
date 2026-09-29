from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id")
w_hist = Window.partitionBy(F.month("order_order_date"), "order_status").orderBy("paid_at_weekofyear", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("order_date", "id")
w4 = Window.partitionBy(F.month("order_date"), "status").orderBy(F.col("paid_at_weekofyear").desc_nulls_last())
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

base = (
    df_payments
    .filter(F.col("paid_at") > F.to_timestamp(F.lit("2023-10-30 04:00:00")))
    .withColumn("paid_at_weekofyear", F.weekofyear("paid_at"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("OTHER")))
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.order_date"), F.col("d.status").alias("order_status"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.paid_at",
        "src.status",
        "src.paid_at_weekofyear",
        "src.order_date",
        F.col("o.order_date").alias("order_order_date"),
    )
)

enriched_payments = (
    with_dims
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.status").alias("order_status"))
)

scored = (
    enriched_payments
    .select(
        "*",
        F.min("paid_at_weekofyear").over(w_running).alias("order_id_min_paid_at_weekofyear"),
        F.avg("paid_at_weekofyear").over(w_hist).alias("paid_at_weekofyear_avg_3"),
        F.sum("paid_at_weekofyear").over(w_running).alias("order_id_sum_paid_at_weekofyear"),
        F.lead("paid_at_weekofyear", 2).over(w3).alias("next_paid_at_weekofyear"),
        F.cume_dist().over(w4).alias("paid_at_weekofyear_cume_dist"),
    )
)

result = (
    scored
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "paid_at_weekofyear",
        "order_date",
        "order_status",
        "paid_at_weekofyear_avg_3",
        "order_id_sum_paid_at_weekofyear",
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
