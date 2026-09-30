from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_scaled").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity_scaled").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity_filled")
w5 = Window.partitionBy("product_id").orderBy("quantity_filled_percent_rank", "id")
w_hist = Window.partitionBy("product_id")
w_recent = Window.partitionBy("product_id").orderBy(F.col("dedup_rank").desc(), "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("quantity_scaled").asc_nulls_first(), "id")
w9 = Window.orderBy("rn", "id").rowsBetween(-2, -1)
w10 = Window.partitionBy("product_id").orderBy("first_known_quantity_scaled", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("first_known_quantity_scaled", F.first("quantity_scaled", ignorenulls=True).over(w3))
    .withColumn("quantity_filled_percent_rank", F.percent_rank().over(w4))
)

scored = (
    enriched
    .withColumn("event_seq_2", F.row_number().over(w5))
    .withColumn("product_id_avg_quantity_filled_percent_rank", F.avg("quantity_filled_percent_rank").over(w_hist))
    .withColumn("event_seq_3", F.row_number().over(w_recent))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

valid = (
    latest
    .filter(F.col("dedup_rank") == 1)
    .withColumn("quantity_filled_percent_rank_times_event_seq_2", (F.col("quantity_filled_percent_rank") * F.col("event_seq_2")))
)

with_dims = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.category"), F.col("d.brand"), F.col("d.stock_qty"))
)

result = (
    with_dims
    .select(
        "*",
        F.avg("stock_qty").over(w9).alias("stock_qty_avg_2_prior"),
        F.ntile(3).over(w10).alias("first_known_quantity_scaled_bucket"),
    )
)
