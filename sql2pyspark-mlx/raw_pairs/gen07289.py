from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock_plus_months").desc(), "id")

base = (
    df_inventory
    .filter(F.col("quantity").between(106, 228))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"))
)

with_windows = (
    joined
    .filter(F.col("store_id") == 3)
    .withColumn("last_known_quantity_rounded", F.last("quantity_rounded", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

kept_keys = (
    with_windows
    .select("id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([1, 2, 8, 10]))
        .select("id", "product_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("quantity") >= 257, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.date_trunc("week", "last_restock").alias("week_start"),
    )
    .agg(
        F.round(F.sum("last_known_quantity_rounded") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("last_known_quantity_rounded_per_london"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("quantity_per_berlin"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
