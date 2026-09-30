from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("greatest_quantity_line_total", "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("next_quantity").asc_nulls_first())
w4 = Window.orderBy("greatest_quantity_line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("seq_no").desc(), "id")
w6 = Window.orderBy("greatest_quantity_line_total", "id").rowsBetween(-5, -1)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([6, 14, 20, 38]))
    .select(
        "id",
        "quantity",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

base_order_items = (
    base
    .filter(F.col("quantity") < 5)
    .withColumn("greatest_quantity_line_total", F.greatest("quantity", "line_total"))
)

scored = (
    base_order_items
    .withColumn("prev_greatest_quantity_line_total", F.lag("greatest_quantity_line_total").over(w))
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("cumulative_greatest_quantity_line_total", F.sum("greatest_quantity_line_total").over(w2))
)

base_order_items_2 = (
    scored
    .filter(F.col("next_quantity") >= 7)
    .select(
        "id",
        "quantity",
        "line_total",
        "greatest_quantity_line_total",
        "next_quantity",
        "seq_no",
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

with_windows = (
    base_order_items_2
    .select(
        "id",
        "line_total",
        "next_quantity",
        "seq_no",
        F.cume_dist().over(w3).alias("next_quantity_cume_dist"),
        F.sum("next_quantity").over(w4).alias("running_next_quantity"),
        F.ntile(5).over(w5).alias("seq_no_bucket"),
        F.avg("next_quantity").over(w6).alias("next_quantity_avg_5_prior"),
    )
)

result = (
    with_windows
    .withColumn("running_next_quantity_tier", F.when(F.col("running_next_quantity") >= 35, "PREMIUM").otherwise("STANDARD"))
    .withColumn("next_quantity_cume_dist_tier", F.when(F.col("next_quantity_cume_dist") >= 0.72, "PREMIUM").otherwise("STANDARD"))
    .cube("running_next_quantity_tier", "next_quantity_cume_dist_tier")
    .agg(
        F.sum("seq_no_bucket").alias("total_seq_no_bucket"),
        F.round(F.avg("next_quantity"), 2).alias("avg_next_quantity"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_next_quantity") > 0)
)
