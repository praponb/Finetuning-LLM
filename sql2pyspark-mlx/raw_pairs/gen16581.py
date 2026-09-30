from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w_recent = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy(F.col("cumulative_quantity").desc(), "id")
w7 = Window.orderBy("order_id_max_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("order_id_max_quantity", "id")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([4, 27]))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "quantity", "quantity_filled")
)

scored = (
    ranked
    .filter(F.col("quantity") == 7)
    .select(
        "*",
        F.lag("quantity", 1).over(w_recent).alias("prev_quantity"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.max("quantity").over(w4).alias("order_id_max_quantity"),
        F.sum("quantity_filled").over(w5).alias("cumulative_quantity_filled"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("id", "quantity", "order_id_max_quantity", "cumulative_quantity_filled")
)

with_windows = (
    latest
    .withColumn("cumulative_cumulative_quantity_filled", F.sum("cumulative_quantity_filled").over(w7))
    .withColumn("prev_cumulative_quantity_filled", F.lag("cumulative_quantity_filled", 2).over(w8))
)

kept_keys = (
    with_windows
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(290, 360))
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .groupBy(
        F.when(F.col("cumulative_quantity_filled") >= 368, "LARGE").otherwise("SMALL").alias("cumulative_quantity_filled_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_quantity_filled", [0.25, 0.5, 0.75]).alias("cumulative_quantity_filled_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("order_id_max_quantity"), 2).alias("avg_order_id_max_quantity"),
    )
)

result = (
    stats
    .select(
        "cumulative_quantity_filled_tier",
        "cumulative_quantity_filled_quartiles",
        "quantity_bit_and",
    )
    .orderBy("cumulative_quantity_filled_tier")
)
