from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("category")
w3 = Window.partitionBy("supplier_id", "brand")
w4 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_windows = (
    df_products
    .filter(F.col("brand").isNull())
    .select(
        "*",
        F.sum("stock_qty").over(w).alias("cumulative_stock_qty"),
        F.corr("cost", "price").over(w_recent).alias("corr_cost_price"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "cumulative_stock_qty",
        "price_share",
        F.sum("cumulative_stock_qty").over(w4).alias("running_cumulative_stock_qty"),
        F.avg("cost").over(w5).alias("cost_avg_2_prior"),
    )
)

base = (
    metrics
    .filter(F.col("launch_date") < "2025-01-01")
    .select(
        "id",
        "brand",
        "launch_date",
        "cumulative_stock_qty",
        "price_share",
        "cost_avg_2_prior",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.launch_date",
        "s.price_share",
        "s.cost_avg_2_prior",
        "s.days_since_launch_date",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

latest_match_products = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.days_since_launch_date",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

result = (
    latest_match_products
    .select("id", "days_since_launch_date", "unit_price")
    .orderBy(
        F.col("unit_price").asc_nulls_first(),
        F.col("days_since_launch_date").desc(),
    )
)
