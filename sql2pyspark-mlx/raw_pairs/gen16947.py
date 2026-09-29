from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("city").orderBy("event_seq", "id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.orderBy("sqft", "id")
w5 = Window.partitionBy(F.year("last_restock"), "city").orderBy("opened_date", "id")
w_hist = Window.partitionBy("product_id").orderBy("prev_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.when(F.col("quantity") >= 256, "High").when(F.col("quantity") >= 86, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

with_windows = (
    valid
    .filter(F.col("product_id").isin([3, 5, 8]))
    .withColumn("event_seq", F.row_number().over(w))
)

enriched = (
    with_windows
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.opened_date"), F.col("d.city"), F.col("d.sqft"))
)

metrics = (
    enriched
    .withColumn("prev_event_seq", F.lag("event_seq").over(w2))
    .withColumn("next_sqft", F.lead("sqft").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
)

with_windows_inventory = (
    metrics
    .withColumn("next_sqft_2", F.lead("sqft", 1, 0).over(w4))
    .withColumn("prev_sqft", F.lag("sqft", 2, 0).over(w5))
    .withColumn("event_seq_bucket", F.ntile(10).over(w2))
    .withColumn("first_known_next_sqft", F.first("next_sqft", ignorenulls=True).over(w_hist))
)

result = (
    with_windows_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["brand", "city"], ["brand"]],
        F.col("p.brand").alias("brand"),
        F.col("a.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.next_sqft")).alias("avg_next_sqft"),
        F.round(F.sum(F.col("a.prev_event_seq")), 2).alias("total_prev_event_seq"),
    )
    .orderBy(F.col("avg_next_sqft").desc_nulls_first())
)
