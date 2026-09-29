from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("order_id").orderBy("first_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy(F.month("order_date"), "order_id").orderBy(F.col("running_min_quantity").desc(), "id")
w8 = Window.partitionBy("order_id").orderBy(F.col("quantity_share").desc_nulls_last(), "id")
w9 = Window.partitionBy("order_id", "channel").orderBy(F.col("running_min_quantity").desc_nulls_first(), "id")

scored = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.min("quantity").over(w).alias("running_min_quantity"),
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.sum("quantity").over(w).alias("running_sum_quantity"),
    )
)

scored_order_items = (
    scored
    .filter(F.col("running_max_quantity").between(5.2, 5.6))
    .select(
        "*",
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.avg("running_max_quantity").over(w3).alias("order_id_avg_running_max_quantity"),
    )
)

joined = (
    scored_order_items
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.order_date"), F.col("p.discount"), F.col("p.channel"))
)

scored_order_items_2 = (
    joined
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w4)), 4).alias("quantity_share"),
        F.first("running_min_quantity", ignorenulls=True).over(w5).alias("first_known_running_min_quantity"),
        F.last("order_id_avg_running_max_quantity", ignorenulls=True).over(w_hist).alias("last_known_order_id_avg_running_max_quantity"),
        F.ntile(5).over(w_hist_2).alias("running_min_quantity_bucket"),
    )
)

cleaned = (
    scored_order_items_2
    .filter(F.col("unit_price") >= 340)
    .select(
        "*",
        (F.col("running_max_quantity") / F.nullif("quantity_share", F.lit(0))).alias("running_max_quantity_per_quantity_share"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_order_items = (
    ranked
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_order_items
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["status", "order_item_channel"], ["status"], []],
        F.col("p.status").alias("status"),
        F.col("e.channel").alias("order_item_channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.running_sum_quantity")).alias("max_running_sum_quantity"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("status").asc_nulls_last())
)
