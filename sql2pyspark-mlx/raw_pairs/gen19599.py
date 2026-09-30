from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "country").orderBy(F.col("cost").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_dims = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.rating"), F.col("s.country"), F.col("s.supplier_name"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "country",
        "supplier_name",
    )
)

latest_match = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.tags",
        "s.supplier_name",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.stock_qty",
        "s.supplier_name",
        "s.unit_price",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock_2"))
)

result = (
    with_last
    .groupBy(
        F.date_trunc("year", "inventory_last_restock_2").alias("year_start"),
        F.date_trunc("quarter", "inventory_last_restock").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min("inventory_quantity").alias("min_inventory_quantity"),
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
