from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Acme"))))
    .withColumn("stock_qty_plus_price", (F.col("stock_qty") + F.col("price")))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

latest = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_last_products = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.tags",
        "s.supplier_id",
        "s.stock_qty_plus_price",
        "s.order_item_quantity",
        F.col("c.sale_date"),
    )
)

summary = (
    with_last_products
    .groupBy(F.date_trunc("week", "sale_date").alias("week_start"))
    .agg(
        F.round(F.avg("order_item_quantity"), 2).alias("avg_order_item_quantity"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.percentile_approx("stock_qty_plus_price", [0.25, 0.5, 0.75]).alias("stock_qty_plus_price_quartiles"),
        F.corr("price", "cost").alias("corr_price_cost"),
        F.percentile_approx("order_item_quantity", [0.25, 0.5, 0.75]).alias("order_item_quantity_quartiles"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    summary
    .select(
        "week_start",
        "distinct_supplier_ids",
        "corr_price_cost",
        "order_item_quantity_quartiles",
        "num_rows",
    )
)
