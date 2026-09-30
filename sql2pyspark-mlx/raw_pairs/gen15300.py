from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w_running = Window.partitionBy("store_id").orderBy("quantity", "id")
w_hist = Window.partitionBy("store_id").orderBy("next_quantity", "id")

deduped = (
    df_inventory
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "product_id", "quantity", "last_restock")
)

with_windows = (
    deduped
    .select(
        "*",
        F.lag("quantity").over(w_running).alias("prev_quantity"),
        F.lead("quantity").over(w_running).alias("next_quantity"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

with_windows_inventory = (
    with_windows
    .filter(F.col("last_restock") > "2025-01-07")
    .select(
        "*",
        F.lead("next_quantity").over(w_hist).alias("next_next_quantity"),
        F.lag("quantity").over(w_hist).alias("prev_quantity_2"),
        F.lead("quantity").over(w_hist).alias("next_quantity_2"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

filtered = (
    with_windows_inventory
    .filter(F.col("product_id") == 5)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "next_quantity",
        "seq_no",
        F.floor("next_quantity").alias("next_quantity_whole"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.opened_date"), F.col("p.store_name"), F.col("p.city"))
)

enriched_inventory = (
    enriched
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.quantity",
        "s.last_restock",
        "s.seq_no",
        "s.opened_date",
        F.col("d.city").alias("store_city"),
        F.col("d.opened_date").alias("store_opened_date"),
        F.col("d.store_name").alias("store_store_name"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        "seq_no",
        "store_city",
        "store_opened_date",
    )
)
