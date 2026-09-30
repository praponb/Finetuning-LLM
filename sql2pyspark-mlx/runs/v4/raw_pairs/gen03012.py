from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2024-03-26").cast("date")) & F.col("product_id").isin([5, 7, 8, 9]))
    .select("*", F.ceil("quantity").alias("quantity_whole"))
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("quantity_whole").over(w).alias("cumulative_quantity_whole"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
        F.lag("quantity", 2, 0).over(w3).alias("prev_quantity"),
    )
)

matched_keys = (
    enriched
    .select("id", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 4, 5]))
        .select("id", "product_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .groupBy(F.date_trunc("QUARTER", "last_restock").alias("quarter_start"))
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("last_known_quantity", 0.9).alias("last_known_quantity_p90"),
        F.max("quantity_whole").alias("max_quantity_whole"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity_whole", [0.25, 0.5, 0.75]).alias("quantity_whole_quartiles"),
    )
)

result = (
    stats
    .select(
        "quarter_start",
        "quantity_bit_or",
        "quantity_quartiles",
        "last_known_quantity_p90",
        "max_quantity_whole",
        "distinct_ids",
    )
    .orderBy(F.col("quantity_bit_or").desc())
)
