from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("line_total", "id")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([8, 35]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "unit_price", "line_total", "quantity_str")
)

with_windows = (
    deduped
    .filter(F.col("unit_price") > 280)
    .select(
        "*",
        F.sum("quantity_str").over(w2).alias("running_quantity_str"),
        F.sum("line_total").over(w_ordered).alias("cumulative_line_total"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "line_total",
        "quantity_str",
        "cumulative_line_total",
        F.lead("quantity_str").over(w4).alias("next_quantity_str"),
        F.lag("quantity_str").over(w4).alias("prev_quantity_str"),
        F.lag("cumulative_line_total").over(w4).alias("prev_cumulative_line_total"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_cumulative_line_total") >= 326, "HIGH").otherwise("LOW").alias("prev_cumulative_line_total_tier"),
    )
    .agg(
        F.round(F.avg("next_quantity_str"), 2).alias("avg_next_quantity_str"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
