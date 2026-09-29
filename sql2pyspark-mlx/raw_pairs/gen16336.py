from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("session_start_flag", "id")
w_part = Window.partitionBy("product_id").orderBy("quantity", "id")
w5 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 8]))
    .withColumn("last_restock_month", F.month("last_restock"))
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("last_restock_period").over(w).isNull() | (F.col("last_restock_period") > F.lag("last_restock_period").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_dims = (
    islands
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.brand"))
)

enriched = (
    with_dims
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_month",
        "session_start_flag",
        "island_id",
        "brand",
        F.lead("island_id", 1).over(w3).alias("next_island_id"),
        F.row_number().over(w_part).alias("seq_no"),
        F.sum("island_id").over(w5).alias("running_island_id"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .filter(F.year(F.col("b.opened_date")) >= 2025)
    .select(
        "e.id",
        "e.store_id",
        "e.last_restock",
        "e.session_start_flag",
        "e.next_island_id",
        "e.seq_no",
        "e.running_island_id",
        F.col("b.region"),
        F.col("b.city"),
        F.col("b.store_name"),
    )
)

with_dims_inventory = (
    joined
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.region").alias("store_region"),
        F.col("b.city").alias("store_city"),
    )
)

result = (
    with_dims_inventory
    .select(
        "id",
        "last_restock",
        "session_start_flag",
        "next_island_id",
        "seq_no",
        "region",
        "city",
        "store_region",
    )
    .orderBy("city", "store_region")
)
