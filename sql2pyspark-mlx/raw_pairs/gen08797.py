from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id", "product_id").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

joined = (
    df_order_items
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.price") < 1060)
    .select("src.*", F.col("d.stock_qty"), F.col("d.category"))
)

with_windows = (
    joined
    .select(
        "*",
        F.max("unit_price").over(w_running).alias("last7_max_unit_price"),
        F.avg("unit_price").over(w_running).alias("last7_avg_unit_price"),
        F.min("unit_price").over(w_running).alias("last7_min_unit_price"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.product_name"), F.col("p.price"), F.col("p.launch_date"))
)

cleaned = (
    with_dims
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.floor("last7_avg_unit_price").alias("last7_avg_unit_price_whole"),
        F.coalesce("category", F.lit("NONE")).alias("resolved_category"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "product_id",
        "quantity",
        "category",
        "last7_max_unit_price",
        "product_name",
    )
)

grouped = (
    deduped
    .groupBy("product_id")
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
        F.sum(F.when(F.col("category") == "Home", F.col("last7_max_unit_price")).otherwise(0)).alias("home_last7_max_unit_price"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
)

result = grouped.select("product_id", "fashion_rows", "quantity_p95")
