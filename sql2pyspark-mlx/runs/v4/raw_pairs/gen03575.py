from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "category").orderBy(F.col("price").desc_nulls_last(), "id")
w2 = Window.partitionBy("brand").orderBy("rn")
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w4 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("rank_in_group", "id")
w6 = Window.orderBy("price", "id")

filtered = (
    df_order_items
    .filter(F.col("unit_price") <= 300)
    .withColumn("quantity_whole", F.ceil("quantity"))
)

enriched = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.price"), F.col("d.category"), F.col("d.brand"))
)

enriched_order_items = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.amount"))
)

deduped = (
    enriched_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped_order_items = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped_order_items
    .select(
        "*",
        F.lag("quantity_whole", 1).over(w3).alias("prev_quantity_whole"),
        F.sum("amount").over(w4).alias("running_amount"),
        F.lag("quantity_whole", 2).over(w5).alias("prev_quantity_whole_2"),
        F.ntile(3).over(w6).alias("price_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_quantity_whole") >= 76, "LARGE").otherwise("SMALL").alias("prev_quantity_whole_tier"),
        F.when(F.col("unit_price") >= 400, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
        "order_id",
    )
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.max("quantity_whole").alias("max_quantity_whole"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.min("unit_price").alias("min_unit_price"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
)
