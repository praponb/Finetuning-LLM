from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy(F.year("launch_date")).orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("category").orderBy("order_item_unit_price", "id")
w6 = Window.orderBy("days_since_launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("category")
w9 = Window.partitionBy(F.month("launch_date"), "category").orderBy(F.col("brand_stddev_samp_cost").desc(), "id")
w_running = Window.partitionBy("category").orderBy("price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("price") < 340)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        F.substring("brand", 1, 4).alias("brand_prefix"),
        F.round("price", 1).alias("price_rounded"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("brand") == "Umbrella")
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "supplier_id",
        "brand_prefix",
        "price_rounded",
        "brand_filled",
        F.stddev_samp("cost").over(w).alias("brand_stddev_samp_cost"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.avg("price_rounded").over(w3).alias("price_rounded_avg_5"),
    )
)

filtered = (
    with_windows
    .filter(F.col("supplier_id").isNotNull() & (F.col("cumulative_cost") > 1485))
    .select(
        "*",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.launch_date",
        "s.brand_prefix",
        "s.price_rounded",
        "s.brand_filled",
        "s.brand_stddev_samp_cost",
        "s.cumulative_cost",
        "s.days_since_launch_date",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

scored = (
    with_latest
    .select(
        "*",
        F.lag("brand_stddev_samp_cost", 1).over(w5).alias("prev_brand_stddev_samp_cost"),
        F.sum("cumulative_cost").over(w6).alias("running_cumulative_cost"),
        F.avg("days_since_launch_date").over(w7).alias("days_since_launch_date_avg_6"),
        F.count("days_since_launch_date").over(w8).alias("category_count_days_since_launch_date"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

scored_products = (
    scored
    .select(
        "id",
        "launch_date",
        "brand_prefix",
        "price_rounded",
        "brand_filled",
        "cumulative_cost",
        "quantity",
        "prev_brand_stddev_samp_cost",
        F.sum("cumulative_cost").over(w_running).alias("running_sum_cumulative_cost"),
        F.min("cumulative_cost").over(w_running).alias("running_min_cumulative_cost"),
        F.count("cumulative_cost").over(w_running).alias("running_count_cumulative_cost"),
    )
)

result = (
    scored_products
    .select(
        "id",
        "running_sum_cumulative_cost",
        "price_rounded",
        "running_min_cumulative_cost",
    )
    .unpivot(
        ids=["id"],
        values=["running_sum_cumulative_cost", "price_rounded", "running_min_cumulative_cost"],
        variableColumnName="metric",
        valueColumnName="val",
    )
)
