from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w_recent = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("running_max_quantity", "id")
w5 = Window.partitionBy("order_id")
w6 = Window.orderBy(F.col("running_count_quantity").desc_nulls_last())
w7 = Window.partitionBy("product_id")

latest = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.count("quantity").over(w_recent).alias("running_count_quantity"),
        F.avg("quantity").over(w_recent).alias("running_avg_quantity"),
        F.sum("quantity").over(w_recent).alias("running_sum_quantity"),
        F.max("quantity").over(w_recent).alias("running_max_quantity"),
        F.lead("unit_price", 1).over(w3).alias("next_unit_price"),
    )
)

latest_order_items = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "dedup_rank",
        "running_count_quantity",
        "running_avg_quantity",
        "running_max_quantity",
    )
)

matched_keys = (
    latest_order_items
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("quantity") < 3)
        .select("id", "product_id")
    )
)

verified = (
    latest_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("order_id_min_running_count_quantity", F.min("running_count_quantity").over(w5))
    .withColumn("running_count_quantity_rank", F.rank().over(w6))
    .withColumn("product_id_max_running_count_quantity", F.max("running_count_quantity").over(w7))
    .withColumn("running_count_quantity_share", F.round((F.col("running_count_quantity") / F.sum("running_count_quantity").over(w7)), 4))
)

base = (
    with_windows
    .filter(F.col("running_max_quantity").between(4.0, 7.7))
    .withColumn("product_id_max_running_count_quantity_adjusted", F.round((F.col("product_id_max_running_count_quantity") * 1.1), 2))
)

with_dims = (
    base
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status").isin(["SHIPPED", "COMPLETED", "RETURNED"]))
    .select("e.*", F.col("o.amount"), F.col("o.status"))
)

result = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.brand").alias("brand"),
        F.col("p.category").alias("category"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("s.unit_price")).alias("total_unit_price"),
        F.min(F.col("s.running_count_quantity_rank")).alias("min_running_count_quantity_rank"),
        F.grouping_id().alias("level_id"),
    )
)
