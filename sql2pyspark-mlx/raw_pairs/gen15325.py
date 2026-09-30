from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("unit_price").desc_nulls_first())
w_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy("last_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("unit_price_dense_rank").desc_nulls_first())

base = (
    df_order_items
    .filter(F.col("order_id") == 1)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 0.9), 2).alias("unit_price_adjusted"),
    )
)

base_order_items = (
    base
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .select(
        "*",
        F.when(F.col("unit_price") >= 390, "High").when(F.col("unit_price") >= 140, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

filtered = (
    base_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "quantity",
        "unit_price",
        "unit_price_band",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    filtered
    .filter(F.col("line_total") < 73)
    .select(
        "id",
        "quantity",
        "unit_price",
        "line_total",
        F.last("unit_price", ignorenulls=True).over(w_hist).alias("last_known_unit_price"),
        F.dense_rank().over(w_grp).alias("unit_price_dense_rank"),
        F.last("quantity", ignorenulls=True).over(w_running).alias("last_known_quantity"),
        F.avg("line_total").over(w_ordered).alias("line_total_avg_5"),
        F.sum("quantity").over(w_running).alias("cumulative_quantity"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("line_total_avg_5").over(w5).alias("running_line_total_avg_5"),
        F.last("last_known_quantity", ignorenulls=True).over(w_running).alias("last_known_last_known_quantity"),
        F.first("unit_price_dense_rank", ignorenulls=True).over(w_hist).alias("first_known_unit_price_dense_rank"),
        F.percent_rank().over(w6).alias("unit_price_dense_rank_percent_rank"),
    )
)

stats = (
    metrics
    .withColumn("first_known_unit_price_dense_rank_tier", F.when(F.col("first_known_unit_price_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("last_known_last_known_quantity_tier", F.when(F.col("last_known_last_known_quantity") >= 7, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["first_known_unit_price_dense_rank_tier", "last_known_last_known_quantity_tier"], ["first_known_unit_price_dense_rank_tier"], []],
        "first_known_unit_price_dense_rank_tier",
        "last_known_last_known_quantity_tier",
    )
    .agg(
        F.sum("line_total").alias("total_line_total"),
        F.percentile_approx("line_total", 0.5).alias("line_total_p50"),
    )
)

result = (
    stats
    .select("total_line_total", "line_total_p50")
    .orderBy(F.col("line_total_p50").desc())
)
