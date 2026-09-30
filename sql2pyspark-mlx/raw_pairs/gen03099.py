from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.stock_qty",
        "s.supplier_id",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_latest
    .withColumn("quarter_start", F.date_trunc("QUARTER", "inventory_last_restock"))
    .withColumn("price_tier", F.when(F.col("price") >= 1270, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "price_tier"], ["quarter_start"]],
        "quarter_start",
        "price_tier",
    )
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.percentile_approx("inventory_quantity", 0.95).alias("inventory_quantity_p95"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
    .orderBy(F.col("books_count").asc_nulls_first())
)
