from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id").orderBy("price_filled_plus_cost", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy(F.month("launch_date")).orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").asc_nulls_first())
w5 = Window.orderBy("price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-09-08").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
        F.col("price").cast("int").alias("price_int"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

enriched = (
    filtered
    .filter(F.col("category").isin(["Sports", "Books", "Grocery"]))
    .select(
        "*",
        F.lag("price_int").over(w).alias("prev_price_int"),
        F.lead("price_int").over(w).alias("next_price_int"),
        F.lag("stock_qty").over(w).alias("prev_stock_qty"),
        F.row_number().over(w).alias("seq_no"),
    )
)

cleaned = (
    enriched
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-06-17"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "price_int",
        "brand_filled",
        "prev_price_int",
        "next_price_int",
        "prev_stock_qty",
        F.when(F.col("price_int") >= 62, "High").when(F.col("price_int") >= 24, "Medium").otherwise("Low").alias("price_int_band"),
        (F.col("price_filled") + F.col("cost")).alias("price_filled_plus_cost"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "prev_price_int",
        "prev_stock_qty",
        F.avg("price_int").over(w2).alias("price_int_avg_3"),
        F.sum("stock_qty").over(w3).alias("running_stock_qty"),
        F.cume_dist().over(w4).alias("stock_qty_cume_dist"),
        F.sum("prev_stock_qty").over(w5).alias("running_prev_stock_qty"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "prev_price_int",
        "price_int_avg_3",
        "running_stock_qty",
        "running_prev_stock_qty",
        F.count("prev_stock_qty").over(w_running).alias("running_count_prev_stock_qty"),
        F.min("prev_stock_qty").over(w_running).alias("running_min_prev_stock_qty"),
        F.max("prev_stock_qty").over(w_running).alias("running_max_prev_stock_qty"),
        F.sum("stock_qty").over(w7).alias("running_stock_qty_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("running_min_prev_stock_qty").alias("RUNNING_MIN_PREV_STOCK_QTY"),
        F.col("running_max_prev_stock_qty").alias("RUNNING_MAX_PREV_STOCK_QTY"),
    )
    .unpivot(
        ids=["id"],
        values=["RUNNING_MIN_PREV_STOCK_QTY", "RUNNING_MAX_PREV_STOCK_QTY"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
