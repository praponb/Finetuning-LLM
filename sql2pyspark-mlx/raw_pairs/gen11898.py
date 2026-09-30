from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "last_restock"), "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id", "product_id").orderBy("next_quantity", "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w6 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w7 = Window.partitionBy("store_id").orderBy("cumulative_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy(F.month("last_restock")).orderBy("first_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_inventory
    .select(
        "*",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
        F.first("quantity", ignorenulls=True).over(w_ordered).alias("first_known_quantity"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.region").isNull())
    .select("s.*", F.col("b.region"))
)

with_windows = (
    joined
    .select(
        "id",
        "store_id",
        F.lead("quantity", 1).over(w_grp).alias("next_quantity_2"),
        F.lead("next_quantity", 2).over(w5).alias("next_next_quantity"),
        F.lag("cumulative_quantity", 1).over(w6).alias("prev_cumulative_quantity"),
        F.first("quantity", ignorenulls=True).over(w7).alias("first_known_quantity_2"),
        F.first("next_quantity", ignorenulls=True).over(w_grp_2).alias("first_known_next_quantity"),
    )
)

joined_inventory = (
    with_windows
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.prev_cumulative_quantity",
        "e.first_known_next_quantity",
        F.col("p.region"),
        F.col("p.city"),
        F.col("p.sqft"),
    )
)

result = (
    joined_inventory
    .alias("s")
    .join(df_stores.alias("dim"), F.col("s.store_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["store_region", "inventory_city"], ["store_region"]],
        F.col("dim.region").alias("store_region"),
        F.col("s.city").alias("inventory_city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.prev_cumulative_quantity")).alias("min_prev_cumulative_quantity"),
        F.avg(F.col("s.first_known_next_quantity")).alias("avg_first_known_next_quantity"),
    )
    .orderBy(F.col("min_prev_cumulative_quantity").asc_nulls_last())
)
