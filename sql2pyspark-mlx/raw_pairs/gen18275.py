from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("product_launch_date_2", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 6]))
    .select(
        "*",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
        F.when(F.col("units") >= 34, "High").when(F.col("units") >= 12, "Medium").otherwise("Low").alias("units_band"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.launch_date"), F.col("d.price"), F.col("d.stock_qty"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.sale_date",
        "e.units",
        "e.revenue",
        "e.launch_date",
        "e.price",
        "e.stock_qty",
        F.col("d.stock_qty").alias("product_stock_qty"),
        F.col("d.category"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "product_id",
        "revenue",
        "launch_date",
        "price",
        "product_stock_qty",
        F.row_number().over(w_grp).alias("seq_no"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
    )
)

enriched_store_sales = (
    scored
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.brand").isin(["Acme", "Umbrella", "Initech"]))
    .select(
        "e.*",
        F.col("d.cost"),
        F.col("d.product_name"),
        F.col("d.launch_date").alias("product_launch_date"),
    )
)

with_dims_store_sales = (
    enriched_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.launch_date").alias("product_launch_date_2"),
        F.col("p.stock_qty"),
    )
)

metrics = (
    with_dims_store_sales
    .withColumn("last7_sum_price", F.sum("price").over(w3))
    .withColumn("last7_avg_price", F.avg("price").over(w3))
    .withColumn("last7_min_price", F.min("price").over(w3))
)

summary = (
    metrics
    .rollup("product_id")
    .agg(
        F.round(F.avg("product_stock_qty"), 2).alias("avg_product_stock_qty"),
        F.min("cost").alias("min_cost"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
    )
)

result = (
    summary
    .select("product_id", "min_cost")
    .orderBy("product_id")
)
