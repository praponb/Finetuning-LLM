from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy("is_singapore", "id").rowsBetween(-3, Window.currentRow)

kept = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", F.col("c.channel"))
)

valid = (
    latest_match
    .filter(F.col("channel").isNotNull())
    .withColumn("is_singapore", F.when(F.col("city") == "Singapore", 1).otherwise(0))
)

latest_match_stores = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

not_flagged = (
    latest_match_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") <= 21), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("channel").eqNullSafe(F.lit("partner")))
    .withColumn("city_prefix", F.substring("city", 1, 4))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("OTHER")))
)

with_windows = (
    base
    .select(
        "id",
        "city",
        "channel",
        "is_singapore",
        "city_prefix",
        F.max("inventory_quantity").over(w3).alias("last4_max_inventory_quantity"),
        F.sum("inventory_quantity").over(w3).alias("last4_sum_inventory_quantity"),
        F.count("inventory_quantity").over(w3).alias("last4_count_inventory_quantity"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "city",
        "last4_max_inventory_quantity",
        "last4_sum_inventory_quantity",
        "last4_count_inventory_quantity",
    )
    .orderBy("last4_max_inventory_quantity")
)
