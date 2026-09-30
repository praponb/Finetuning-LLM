from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("unit_price").asc_nulls_last())
w2 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([3, 18]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.order_date"), F.col("d.amount"), F.col("d.channel"))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "quantity", "unit_price", "line_total", "order_date", "amount")
)

metrics = (
    ranked
    .filter(F.col("line_total") < 21)
    .select(
        "id",
        "quantity",
        "line_total",
        "amount",
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.sum("line_total").over(w3).alias("running_line_total"),
    )
)

base = (
    metrics
    .filter(F.col("running_line_total") != 188)
    .withColumn("first_known_quantity_per_amount", F.round((F.col("first_known_quantity") / F.col("amount")), 2))
)

stats = (
    base
    .groupBy(
        F.when(F.col("amount") >= 1550, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.when(F.col("first_known_quantity_per_amount") >= 84, "HIGH").otherwise("LOW").alias("first_known_quantity_per_amount_tier"),
        F.when(F.col("quantity") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("running_line_total"), 2).alias("avg_running_line_total"),
        F.avg("quantity").alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("running_line_total", [0.25, 0.5, 0.75]).alias("running_line_total_quartiles"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    stats
    .select(
        "amount_tier",
        "first_known_quantity_per_amount_tier",
        "quantity_tier",
        "event_count",
        "avg_running_line_total",
        "quantity_stddev",
        "running_line_total_quartiles",
    )
)
