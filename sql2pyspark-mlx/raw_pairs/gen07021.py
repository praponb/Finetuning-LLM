from pyspark.sql import functions as F

quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

base = (
    df_store_sales
    .filter(F.col("units") < 23)
    .select(
        "*",
        F.when(F.col("revenue") >= 3500, "High").when(F.col("revenue") >= 1400, "Medium").otherwise("Low").alias("revenue_level"),
        F.col("units").cast("double").alias("units_num"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.category"), F.col("b.launch_date"))
)

joined_store_sales = (
    joined
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.price") >= 580)
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.units",
        "src.revenue",
        "src.units_num",
        "src.launch_date",
        F.col("b.launch_date").alias("product_launch_date"),
        F.col("b.price"),
    )
)

remaining_keys = (
    joined_store_sales
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("units") > 19)
        .select("id")
    )
)

reconciled = (
    joined_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "units_num",
        "launch_date",
        "product_launch_date",
        "price",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("revenue").desc_nulls_last())
)
