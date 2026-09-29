from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_scaled").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.orderBy(F.col("last_restock_dayofweek").asc_nulls_first())
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

valid = (
    df_inventory
    .filter(F.col("quantity").between(85, 101))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    ranked
    .select("store_id", "product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity").between(70, 160))
        .select("store_id", "product_id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("store_id", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") != 154)
        .select("store_id", "product_id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("product_id_count_last_restock_dayofweek", F.count("last_restock_dayofweek").over(w2))
    .withColumn("last_restock_dayofweek_rank", F.rank().over(w3))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w4))
)

result = (
    enriched
    .withColumn("last_restock_dayofweek_tier", F.when(F.col("last_restock_dayofweek") >= 81, "LARGE").otherwise("SMALL"))
    .cube("last_restock_dayofweek_tier", "store_id", "product_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("product_id_count_last_restock_dayofweek", [0.25, 0.5, 0.75]).alias("product_id_count_last_restock_dayofweek_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("quantity_bit_and") < 1000)
    .orderBy("avg_quantity")
)
