from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

enriched = (
    latest_match
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.*", F.col("p.supplier_name"), F.col("p.rating"), F.col("p.city"))
)

metrics = (
    enriched
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "quantity",
        "supplier_name",
        "rating",
        "city",
        F.min("rating").over(w2).alias("last4_min_rating"),
        F.sum("rating").over(w2).alias("last4_sum_rating"),
        F.avg("rating").over(w2).alias("last4_avg_rating"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

totals = (
    with_latest
    .groupBy(
        "city",
        F.when(F.col("last4_sum_rating") >= 20.3, "HIGH").otherwise("LOW").alias("last4_sum_rating_tier"),
        F.date_trunc("week", "launch_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select("city", "last4_sum_rating_tier", "week_start", "singapore_count")
    .orderBy(F.col("week_start").desc_nulls_last(), "singapore_count")
)
