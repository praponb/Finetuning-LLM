from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("attr_key", "v"))
)

lines_carts = lines.select("id", "attr_key", "v", F.explode("coupon_codes").alias("value"))

base = (
    lines_carts
    .filter(F.col("value").isNotNull() & ~F.col("attr_key").isin(["device", "weight_kg", "gift"]))
    .select("*", F.upper("attr_key").alias("attr_key_upper"))
)

cleaned = (
    base
    .filter(F.col("attr_key") != "gift")
    .select(
        "id",
        "attr_key",
        "v",
        "value",
        "attr_key_upper",
        F.trim("value").alias("clean_value"),
    )
)

result = (
    cleaned
    .groupBy("attr_key", "value")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count(F.when(F.col("value") == "VIP", 1)).alias("vip_rows"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_count"),
    )
    .orderBy(F.col("event_count").desc())
)
