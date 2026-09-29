from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "region").orderBy(F.col("last_restock").asc_nulls_last())
w2 = Window.partitionBy("region").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("region").orderBy("quantity_bucket_2", "id")
w_running = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

joined = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.sqft") != 13534)
    .select(
        "e.id",
        "e.store_id",
        "e.quantity",
        "e.last_restock",
        F.col("b.opened_date"),
        F.col("b.region"),
    )
)

latest = (
    joined
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "store_id",
        "quantity",
        "opened_date",
        "region",
        "dedup_rank",
        F.ntile(3).over(w2).alias("quantity_bucket"),
        F.ntile(4).over(w2).alias("quantity_bucket_2"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.ntile(10).over(w3).alias("quantity_bucket_2_bucket"),
        F.percent_rank().over(w_running).alias("quantity_percent_rank"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.sqft"))
)

joined_inventory = (
    with_dims
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.quantity",
        "src.opened_date",
        "src.dedup_rank",
        "src.quantity_bucket_2_bucket",
        "src.quantity_percent_rank",
        F.col("s.region").alias("store_region"),
    )
)

result = (
    joined_inventory
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "opened_date",
        "quantity_percent_rank",
        "store_region",
        F.when(F.col("store_region").rlike("^(North|Central)$"), "OTHER_ZONE").when(F.col("store_region").rlike("^(South|East)$"), "GROUP_A").otherwise("EXTENDED").alias("store_region_zone"),
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("store_region_zone").desc_nulls_first())
)
