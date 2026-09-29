from pyspark.sql import functions as F

amount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("amount").alias("sum_order_amount"))

valid = (
    df_inventory
    .filter(F.col("quantity") <= 76)
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

base = (
    valid
    .filter(F.col("last_restock") >= F.lit("2023-07-09").cast("date"))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.date_add("last_restock", 2).alias("last_restock_plus_2d"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled_2"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.last_restock_plus_2d",
        "s.quantity_filled_2",
        F.col("d.opened_date"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.opened_date",
        F.col("p.city"),
        F.col("p.region"),
    )
)

joined_inventory = (
    enriched
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.region",
        F.col("d.opened_date").alias("store_opened_date"),
        F.col("d.region").alias("store_region"),
    )
)

result = (
    joined_inventory
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "region",
        "store_opened_date",
        F.when(F.col("region").rlike("^(East|Central)$"), "GROUP_B").otherwise("GROUP_A").alias("region_zone"),
        F.col("sum_order_amount"),
    )
)
