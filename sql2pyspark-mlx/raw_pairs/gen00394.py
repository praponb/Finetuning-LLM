from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("is_vip").desc_nulls_first())
w2 = Window.partitionBy(F.month("created_at"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("element") == "BUNDLE"))
    .withColumn("is_vip", F.when(F.col("element") == "VIP", 1).otherwise(0))
    .withColumn("resolved_element", F.coalesce("element", F.lit("DIRECT")))
)

scored = (
    valid
    .withColumn("is_vip_cume_dist", F.cume_dist().over(w))
    .withColumn("created_at_month_min_is_vip", F.min("is_vip").over(w2))
)

filtered = (
    scored
    .filter(F.col("element").isNotNull())
    .withColumn("least_is_vip_created_at_month_min_is_vip", F.least("is_vip", "created_at_month_min_is_vip"))
)

result = (
    filtered
    .withColumn("created_at_month_min_is_vip_tier", F.when(F.col("created_at_month_min_is_vip") >= 70, "HIGH").otherwise("LOW"))
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["created_at_month_min_is_vip_tier", "activity_day"], ["created_at_month_min_is_vip_tier"]],
        "created_at_month_min_is_vip_tier",
        "activity_day",
    )
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("element") == "BUNDLE").alias("bundle_events"),
        F.size(F.collect_set("element")).alias("num_elements"),
    )
    .filter(F.col("num_elements") > 1)
)
