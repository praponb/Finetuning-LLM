from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w4 = Window.orderBy("running_sum_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("next_quantity").desc(), "id")
w_ordered = Window.partitionBy("store_id")
w7 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w8 = Window.partitionBy("product_id", "city").orderBy(F.col("running_max_quantity").desc(), "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

cleaned = (
    df_inventory
    .filter(F.col("quantity") != 110)
    .select("*", F.col("quantity").cast("string").alias("quantity_str"))
)

scored = (
    cleaned
    .filter(F.col("last_restock") < "2023-05-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_str",
        F.max("quantity").over(w_recent).alias("running_max_quantity"),
        F.min("quantity").over(w_recent).alias("running_min_quantity"),
        F.sum("quantity").over(w_recent).alias("running_sum_quantity"),
        F.lead("quantity", 2).over(w_running).alias("next_quantity"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.last("running_min_quantity", ignorenulls=True).over(w4).alias("last_known_running_min_quantity"),
        F.ntile(4).over(w5).alias("next_quantity_bucket"),
        F.sum("running_max_quantity").over(w_ordered).alias("store_id_sum_running_max_quantity"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.opened_date"), F.col("s.sqft"), F.col("s.city"))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.quantity_str",
        "s.running_sum_quantity",
        "s.next_quantity",
        "s.last_known_running_min_quantity",
        "s.next_quantity_bucket",
        "s.store_id_sum_running_max_quantity",
        "s.sqft",
        "s.rn",
        F.col("d.product_name"),
        F.col("d.price"),
        F.col("d.launch_date"),
    )
)

result = (
    joined
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_str",
        "next_quantity",
        "last_known_running_min_quantity",
        "store_id_sum_running_max_quantity",
        "sqft",
        "rn",
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("max_order_item_unit_price").desc())
)
