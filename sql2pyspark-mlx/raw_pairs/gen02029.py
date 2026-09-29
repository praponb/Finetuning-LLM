from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w_hist = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

cleaned = (
    deduped
    .filter(F.col("order_id").isin([15, 19, 32]))
    .select("*", (F.col("quantity") + F.col("unit_price")).alias("line_total"))
)

enriched = (
    cleaned
    .filter(F.col("quantity") > 6)
    .select(
        "*",
        F.min("line_total").over(w_hist).alias("running_min_line_total"),
        F.avg("line_total").over(w_hist).alias("running_avg_line_total"),
        F.max("line_total").over(w_hist).alias("running_max_line_total"),
    )
)

enriched_order_items = (
    enriched
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.order_date"), F.col("d.status"))
)

stats = (
    enriched_order_items
    .withColumn("running_avg_line_total_tier", F.when(F.col("running_avg_line_total") >= 69, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_avg_line_total_tier", "product_id", "order_id"], ["running_avg_line_total_tier", "product_id"], ["running_avg_line_total_tier"]],
        "running_avg_line_total_tier",
        "product_id",
        "order_id",
    )
    .agg(
        F.round(F.avg("running_max_line_total"), 2).alias("avg_running_max_line_total"),
        F.percentile_approx("running_avg_line_total", 0.5).alias("running_avg_line_total_p50"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") >= 0)
)

result = (
    stats
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "product_id",
        "order_id",
        "avg_running_max_line_total",
        "running_avg_line_total_p50",
        "min_unit_price",
        F.col("avg_shipment_cost"),
    )
)
