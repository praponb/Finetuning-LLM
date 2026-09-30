from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_first(), "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")

deduped = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    deduped
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.channel"))
)

deduped_order_items = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    deduped_order_items
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand").alias("brand"), F.col("s.channel").alias("channel"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.quantity")).alias("max_quantity"),
    )
)
