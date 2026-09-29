from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy(F.col("last_restock_month_end").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "last_restock_month_end"), "store_id").orderBy(F.col("last_restock_month_end").desc(), "id")
w_hist = Window.partitionBy("store_id").orderBy("last_restock_month_end", "id")

valid = (
    df_inventory
    .filter(F.col("quantity").between(101, 108))
    .select(
        "id",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

scored = (
    valid
    .select(
        "id",
        "store_id",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w_hist).alias("event_seq_2"),
        F.row_number().over(w_ordered).alias("event_seq_3"),
        F.row_number().over(w_ordered).alias("event_seq_4"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.store_name"), F.col("d.city"))
)

joined = (
    enriched
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.opened_date"))
)

remaining_keys = (
    joined
    .select("id")
    .exceptAll(
        df_inventory
        .filter(F.month("last_restock").isin([3, 7, 10]))
        .select("id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .groupingSets(
        [["region", "inventory_city"], ["region"], []],
        F.col("d.region").alias("region"),
        F.col("s.city").alias("inventory_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.event_seq")).alias("max_event_seq"),
    )
)
