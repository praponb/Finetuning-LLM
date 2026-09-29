from pyspark.sql import functions as F

with_dims = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"), F.col("p.price"), F.col("p.cost"))
)

filtered = (
    with_dims
    .filter((~F.col("category").eqNullSafe(F.lit("Electronics"))) & (F.col("last_restock") > F.lit("2023-03-11").cast("date")))
    .withColumn("price_plus_quantity", (F.col("price") + F.col("quantity")))
)

remaining_keys = (
    filtered
    .select("id", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("last_restock") < "2025-09-01")
        .select("id", "product_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.quantity",
        "e.last_restock",
        "e.category",
        "e.cost",
        F.col("s.store_name"),
    )
)

base = (
    enriched
    .filter((F.col("last_restock") >= F.lit("2023-03-13").cast("date")) & F.col("category").isNotNull())
    .select(
        "id",
        "store_id",
        "quantity",
        "store_name",
        F.substring("category", 1, 2).alias("category_prefix"),
    )
)

result = (
    base
    .select(
        "store_name",
        "category_prefix",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Mall Central)$"), "GROUP_A").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("store_name_zone"),
    )
)
