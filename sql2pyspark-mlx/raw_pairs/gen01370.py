from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("unit_price_share", "id")
w4 = Window.partitionBy("order_id").orderBy("last3_count_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("last3_count_quantity").desc_nulls_last())

metrics = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        F.max("quantity").over(w).alias("last3_max_quantity"),
        F.min("quantity").over(w).alias("last3_min_quantity"),
        F.count("quantity").over(w).alias("last3_count_quantity"),
        F.avg("quantity").over(w).alias("last3_avg_quantity"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.status"), F.col("d.channel"), F.col("d.amount"))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "last3_count_quantity", "last3_avg_quantity", "status")
)

metrics_order_items = (
    latest
    .select(
        "*",
        F.sum("last3_count_quantity").over(w4).alias("running_last3_count_quantity"),
        F.avg("last3_avg_quantity").over(w2).alias("order_id_avg_last3_avg_quantity"),
        F.dense_rank().over(w5).alias("last3_count_quantity_dense_rank"),
    )
)

base = (
    metrics_order_items
    .filter(F.col("status").isNotNull())
    .withColumn("last3_count_quantity_whole", F.ceil("last3_count_quantity"))
    .withColumn("status_clean", F.coalesce("status", F.lit("DIRECT")))
)

result = (
    base
    .select(
        "id",
        "order_id",
        "order_id_avg_last3_avg_quantity",
        "last3_count_quantity_dense_rank",
        "status_clean",
    )
)
