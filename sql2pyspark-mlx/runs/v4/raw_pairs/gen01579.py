from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").desc_nulls_first(), "id")
w2 = Window.partitionBy("category").orderBy("unit_price", "id")
w3 = Window.partitionBy("category", "order_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "category").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("stark_count").desc())

filtered = (
    df_order_items
    .filter(F.col("unit_price").between(280, 350))
    .select(
        "*",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.category"), F.col("b.launch_date"))
)

scored = (
    enriched
    .withColumn("unit_price_bucket", F.ntile(5).over(w2))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w3))
    .withColumn("first_known_category", F.first("category", ignorenulls=True).over(w4))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w5))
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.amount"))
)

joined_order_items = (
    joined
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.launch_date").alias("product_launch_date"),
        F.col("p.brand"),
    )
)

joined_order_items_2 = (
    joined_order_items
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.stock_qty"), F.col("d.cost"))
)

result = (
    joined_order_items_2
    .groupBy(
        "product_id",
        "order_id",
        F.when(F.col("unit_price_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("unit_price_bucket_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
    .withColumn("stark_count_rank", F.rank().over(w6))
    .orderBy(F.col("stark_count").desc_nulls_last())
)
