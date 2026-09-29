from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("store_name").like("Old%") & F.col("region").isNotNull())
    .select(
        "*",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.substring("city", 1, 4).alias("city_prefix"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([4, 6])), "left_anti")
)

matched_keys = (
    kept
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("region") == "East")
        .select("city")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.month(F.col("ch.order_date")).isin([1, 6, 11])), "left_anti")
)

kept_keys = (
    eligible
    .select("region", "id")
    .intersectAll(
        df_stores
        .filter(F.col("city") == "Singapore")
        .select("region", "id")
    )
)

verified = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("city") == "London")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "opened_date_plus_months",
        "city_prefix",
        F.date_trunc("year", "opened_date_plus_months").alias("opened_date_plus_months_period"),
    )
)

result = (
    valid
    .select(
        "id",
        "city",
        "region",
        "opened_date_plus_months",
        "city_prefix",
        "opened_date_plus_months_period",
    )
    .orderBy(F.col("city").desc_nulls_last(), "region")
)
