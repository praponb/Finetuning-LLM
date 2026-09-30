from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("sqft_filled_minus_discount").desc())
w3 = Window.partitionBy("region")

valid = (
    df_stores
    .filter(F.col("sqft") >= 7611)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.sqft_filled",
        F.col("c.order_date").alias("order_order_date"),
        F.col("c.discount"),
    )
)

valid_stores = (
    latest_match
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "sqft_filled",
        "discount",
        F.concat_ws(",", "store_name", "region").alias("store_name_region_key"),
        (F.col("sqft_filled") - F.col("discount")).alias("sqft_filled_minus_discount"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

matched_keys = (
    valid_stores
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("sqft").isNull())
        .select("id")
    )
)

verified = (
    valid_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("region").isNotNull() & (F.col("sqft_filled_minus_discount") <= 53))
    .withColumn("region_upper", F.upper("region"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("UNKNOWN")))
)

scored = (
    cleaned
    .withColumn("sqft_filled_minus_discount_percent_rank", F.percent_rank().over(w2))
    .withColumn("region_min_discount", F.min("discount").over(w3))
    .withColumn("region_sum_sqft_filled", F.sum("sqft_filled").over(w3))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([4, 7])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "sqft_filled",
        "store_name_region_key",
        "sqft_filled_minus_discount",
        "region_upper",
        "resolved_region",
        "sqft_filled_minus_discount_percent_rank",
        "region_min_discount",
    )
    .orderBy(F.col("sqft_filled").asc_nulls_last())
)
