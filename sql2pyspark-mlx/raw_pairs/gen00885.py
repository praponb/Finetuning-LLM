from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "customer_id").orderBy("created_at", "id", "attr_key")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

latest = (
    exploded
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "coupon_codes", "attr_key", "v")
)

filtered = (
    latest
    .filter(F.col("customer_id") == 6)
    .select(
        "*",
        F.trim("attr_key").alias("clean_attr_key"),
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_filled"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(~F.col("b.city").isin(["Berlin", "Singapore", "Tokyo"]))
    .select("src.*", F.col("b.country"), F.col("b.segment"), F.col("b.email"))
)

result = (
    enriched
    .groupBy("segment")
    .agg(
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count_if(F.col("attr_key") == "gift").alias("gift_events"),
    )
    .filter(F.col("gift_events") >= 1)
)
