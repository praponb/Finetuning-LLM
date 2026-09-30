from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("supplier_id", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("launch_date_period", "id")
w3 = Window.partitionBy("category").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("brand").orderBy("running_count_stock_qty", "id")
w7 = Window.partitionBy("brand")
w8 = Window.orderBy(F.col("running_sum_stock_qty").asc_nulls_last())

scored = (
    df_products
    .filter(F.col("category") == "Books")
    .withColumn("running_sum_stock_qty", F.sum("stock_qty").over(w_ordered))
    .withColumn("running_count_stock_qty", F.count("stock_qty").over(w_ordered))
    .withColumn("running_avg_stock_qty", F.avg("stock_qty").over(w_ordered))
)

filtered = (
    scored
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-03-27")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "running_sum_stock_qty",
        "running_count_stock_qty",
        F.date_trunc("YEAR", "launch_date").alias("launch_date_period"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.launch_date_period")), "left_anti")
)

gaps = (
    not_flagged
    .withColumn("session_start_flag", F.when(F.lag("launch_date_period").over(w2).isNull() | (F.col("launch_date_period") > F.lag("launch_date_period").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "launch_date",
        "tags",
        "running_sum_stock_qty",
        "running_count_stock_qty",
        "launch_date_period",
        F.sum("session_start_flag").over(w3).alias("island_id"),
    )
)

latest_match = (
    sessionized
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.revenue"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

with_windows = (
    with_latest
    .select(
        "id",
        "tags",
        "running_count_stock_qty",
        "launch_date_period",
        "island_id",
        "units",
        "revenue",
        "inventory_quantity",
        F.row_number().over(w_part).alias("row_seq"),
        F.avg("running_count_stock_qty").over(w7).alias("brand_avg_running_count_stock_qty"),
        F.dense_rank().over(w8).alias("running_sum_stock_qty_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "running_count_stock_qty",
        "launch_date_period",
        "island_id",
        "revenue",
        "inventory_quantity",
        "row_seq",
    )
)
