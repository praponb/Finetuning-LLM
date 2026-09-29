from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("store_id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity") > 158)
        .select("store_id", "product_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("last_restock") > F.lit("2023-05-05").cast("date")) & (F.col("last_restock") <= "2025-06-01"))
    .select(
        "*",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.opened_date") > "2025-07-01")
    .select(
        "s.store_id",
        "s.quantity",
        "s.last_restock",
        "s.quantity_filled",
        "s.quantity_whole",
        F.col("b.region"),
        F.col("b.store_name"),
        F.col("b.opened_date"),
    )
)

base = (
    joined
    .filter((F.col("last_restock") >= F.lit("2023-06-04").cast("date")) & F.col("region").isin(["East", "West", "Central"]))
    .withColumn("quantity_minus_quantity_filled", (F.col("quantity") - F.col("quantity_filled")))
)

result = (
    base
    .select(
        "store_id",
        "quantity",
        "quantity_whole",
        "region",
        "store_name",
        "quantity_minus_quantity_filled",
    )
    .orderBy(F.col("quantity_minus_quantity_filled").desc())
)
