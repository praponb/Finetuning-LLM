from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("running_max_stock_qty").asc_nulls_first(), "id")
w3 = Window.orderBy("running_count_stock_qty", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id").orderBy("running_count_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_order_items
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.stock_qty"))
)

enriched = (
    joined
    .filter(F.col("product_id") == 6)
    .select(
        "id",
        "product_id",
        "quantity",
        "stock_qty",
        F.count("stock_qty").over(w_ordered).alias("running_count_stock_qty"),
        F.sum("stock_qty").over(w_ordered).alias("running_sum_stock_qty"),
        F.avg("stock_qty").over(w_ordered).alias("running_avg_stock_qty"),
        F.max("stock_qty").over(w_ordered).alias("running_max_stock_qty"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

joined_order_items = (
    ranked
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        "a.stock_qty",
        "a.running_count_stock_qty",
        "a.running_avg_stock_qty",
        F.col("d.cost"),
        F.col("d.stock_qty").alias("product_stock_qty"),
    )
)

valid = (
    joined_order_items
    .filter(F.col("stock_qty") <= 337)
    .withColumn("running_count_stock_qty_rounded", F.round("running_count_stock_qty", 2))
)

with_windows = (
    valid
    .select(
        "id",
        "quantity",
        "running_count_stock_qty",
        "running_avg_stock_qty",
        "product_stock_qty",
        F.avg("product_stock_qty").over(w3).alias("product_stock_qty_avg_3"),
        F.max("running_count_stock_qty").over(w4).alias("product_id_max_running_count_stock_qty"),
        F.sum("cost").over(w5).alias("running_cost"),
    )
)

valid_order_items = (
    with_windows
    .filter(F.col("product_stock_qty_avg_3") < 280)
    .select(
        "*",
        F.round((F.col("running_cost") * 1.07), 2).alias("running_cost_adjusted"),
    )
)

result = (
    valid_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.when(F.col("running_cost_adjusted") >= 65, "PREMIUM").otherwise("STANDARD").alias("running_cost_adjusted_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("product_stock_qty_avg_3"), 2).alias("avg_product_stock_qty_avg_3"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.round(F.avg("running_cost_adjusted"), 2).alias("avg_running_cost_adjusted"),
    )
    .filter(F.col("quantity_bit_and") < 1000)
)
