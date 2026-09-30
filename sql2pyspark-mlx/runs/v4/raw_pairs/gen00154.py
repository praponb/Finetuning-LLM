from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("num_attributes") >= 90, "LARGE").otherwise("SMALL").alias("num_attributes_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.max("created_at").alias("last_seen"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
