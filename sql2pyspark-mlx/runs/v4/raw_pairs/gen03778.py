from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .cube("attr_key")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_count"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_ids") < 1)
)
