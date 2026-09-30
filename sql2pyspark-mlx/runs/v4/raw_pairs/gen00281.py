from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "region")
w_part = Window.partitionBy("city")
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_recent = Window.partitionBy("region").orderBy("quantity", "id")
w5 = Window.partitionBy("region")
w6 = Window.partitionBy("city").orderBy(F.col("city_stddev_samp_quantity").desc_nulls_first())

with_dims = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.opened_date"), F.col("d.city"), F.col("d.region"))
)

base = (
    with_dims
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.datediff("last_restock", "opened_date").alias("days_opened_date_to_last_restock"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

metrics = (
    base
    .withColumn("product_id_min_quantity", F.min("quantity").over(w))
    .withColumn("city_stddev_samp_quantity", F.stddev_samp("quantity").over(w_part))
    .withColumn("prev_quantity", F.lag("quantity", 2, 0).over(w3))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

joined = (
    metrics
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"), F.col("p.cost"))
)

kept_keys = (
    joined
    .select("product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([2, 4, 5, 10]))
        .select("product_id", "store_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

metrics_inventory = (
    reconciled
    .select(
        "*",
        F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4).alias("event_seq_share"),
        F.cume_dist().over(w6).alias("city_stddev_samp_quantity_cume_dist"),
    )
)

joined_inventory = (
    metrics_inventory
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.category"), F.col("d.stock_qty"))
)

result = (
    joined_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "opened_date",
        "city",
        "region",
        "days_opened_date_to_last_restock",
        "city_filled",
        F.when(F.col("region").rlike("^(Central|West)$"), "PRIORITY").when(F.col("region").rlike("^(North|South)$"), "CORE").otherwise("GROUP_B").alias("region_zone"),
    )
    .orderBy("store_id")
)
