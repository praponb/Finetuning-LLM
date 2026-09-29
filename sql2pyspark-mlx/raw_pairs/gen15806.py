from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("region").orderBy(F.col("sqft_rounded").desc_nulls_first())

filtered = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-10-06").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"),
        F.round("sqft", 1).alias("sqft_rounded"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.customer_id").isin([10, 12, 15])), "left_anti")
)

matched_keys = (
    eligible
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("sqft") != 9869)
        .select("region", "id")
    )
)

verified = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("store_name").like("Air%"))
    .select("*", F.cume_dist().over(w_hist).alias("sqft_rounded_cume_dist"))
)

filtered_stores = (
    scored
    .filter(F.col("region").isNotNull() & (F.col("sqft_rounded") <= 46))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "store_name_filled",
        "sqft_rounded",
        F.regexp_replace("store_name", " ", "").alias("store_name_slug"),
    )
)

eligible_stores = (
    filtered_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 3, 5])), "left_anti")
)

result = (
    eligible_stores
    .select("id", "store_name", "city", "region", "sqft_rounded", "store_name_slug")
    .orderBy(F.col("id").desc(), "region")
)
