from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_month_end", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock_plus_66d").asc_nulls_first(), "id")
w3 = Window.orderBy("last_restock_plus_66d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("last_restock_month_end"), "store_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last_restock_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-07-11").cast("date")) & (F.col("store_id") == 3))
    .withColumn("last_restock_plus_66d", F.date_add("last_restock", 66))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock_plus_66d",
        "last_restock_month_end",
    )
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .filter(F.col("last_restock_month_end") >= "2025-10-01")
    .select(
        "*",
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.first("quantity", ignorenulls=True).over(w4).alias("first_known_quantity"),
        F.sum("quantity").over(w5).alias("cumulative_quantity"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.store_name"), F.col("d.region"))
)

result = (
    enriched
    .groupBy("store_id", "region")
    .agg(
        F.sum(F.when(F.col("region") == "South", F.col("last_known_quantity")).otherwise(0)).alias("south_last_known_quantity"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("first_known_quantity") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("first_known_quantity_per_north"),
        F.countDistinct("region").alias("distinct_regions"),
    )
    .orderBy(F.col("distinct_regions").desc())
)
