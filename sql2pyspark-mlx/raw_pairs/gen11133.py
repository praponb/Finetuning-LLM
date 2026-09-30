from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w_part = Window.partitionBy(F.year("last_restock"), "city").orderBy("quantity", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("region").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select("*", F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    latest
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity",
        "s.last_restock",
        F.col("p2.region"),
        F.col("p2.city"),
    )
)

scored = (
    joined
    .filter(F.col("product_id") == 3)
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "city",
        F.avg("quantity").over(w_part).alias("quantity_avg_2_prior"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.first("region", ignorenulls=True).over(w4).alias("first_known_region"),
    )
)

result = (
    scored
    .groupBy("city", "product_id", "store_id")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("cumulative_quantity")).otherwise(0)).alias("berlin_cumulative_quantity"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
)
