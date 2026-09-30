from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region") == "Central")
    .select(
        "*",
        F.lower("store_name").alias("store_name_lower"),
        F.trim("store_name").alias("clean_store_name"),
    )
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("sqft") > 14779)
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") >= F.col("s.opened_date")), "left_semi")
)

matched_keys = (
    not_flagged
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("city").isin(["London", "Bangkok"]))
        .select("city")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id").isin([2, 4, 6, 10])), "left_anti")
)

result = (
    eligible
    .select("id", "store_name", "region", "opened_date", "sqft", "clean_store_name")
    .orderBy(F.col("region").desc_nulls_first())
)
