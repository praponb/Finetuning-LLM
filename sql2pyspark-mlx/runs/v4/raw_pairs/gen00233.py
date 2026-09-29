from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

flattened = lines.select("id", "customer_id", F.explode("coupon_codes").alias("value_2"))

valid = (
    flattened
    .filter(F.col("value_2").isNotNull())
    .select(
        "*",
        F.length("value_2").alias("value_2_length"),
        F.coalesce("value_2", F.lit("DIRECT")).alias("value_2_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("value_2_length") >= 78, "LARGE").otherwise("SMALL").alias("value_2_length_tier"),
        "customer_id",
    )
    .agg(
        F.sort_array(F.collect_set("value_2")).alias("value_2_list"),
        F.sort_array(F.collect_set("value_2")).alias("value_2_list_2"),
        F.round(F.sum("value_2_length") / F.nullif(F.count(F.when(F.col("value_2") == "WELCOME5", 1)), F.lit(0)), 2).alias("value_2_length_per_welcome5"),
        F.round(F.sum("value_2_length") / F.nullif(F.count(F.when(F.col("value_2") == "VIP", 1)), F.lit(0)), 2).alias("value_2_length_per_vip"),
    )
)
