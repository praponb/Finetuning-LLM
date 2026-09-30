from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id", "category").orderBy(F.col("order_item_quantity").asc_nulls_last())
w_recent = Window.partitionBy("supplier_id", "category").orderBy("order_item_quantity", "id").rowsBetween(-6, Window.currentRow)
w_recent_2 = Window.partitionBy("category").orderBy(F.col("price").desc_nulls_first())
w_grp = Window.partitionBy("category").orderBy("dedup_rank", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

eligible = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") < 84), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.tags",
        "s.supplier_id",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

deduped = (
    latest_match
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([12, 24, 40])), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.avg("price").over(w_recent).alias("price_avg_6"),
        F.cume_dist().over(w_recent_2).alias("price_cume_dist"),
        F.avg("price").over(w_grp).alias("price_avg_3"),
    )
)

latest_match_products = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    latest_match_products
    .groupBy("supplier_id")
    .pivot("category", ["Books", "Sports"])
    .agg(F.sum("price_cume_dist").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "supplier_id",
        F.col("Books_total").alias("books_total"),
        F.col("Books_item_count").alias("books_item_count"),
        F.col("Sports_total").alias("sports_total"),
        F.col("Sports_item_count").alias("sports_item_count"),
    )
)
