from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("k").orderBy("is_spring10_flag", "id", "k")
w_part_2 = Window.partitionBy("customer_id").orderBy("is_spring10_flag", "id", "k")
w3 = Window.partitionBy("customer_id").orderBy(F.col("is_spring10_flag").desc(), "id", "k")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("k").isNull())
    .select(
        "*",
        F.initcap(F.trim("k")).alias("k_title"),
        F.array_contains("coupon_codes", "SPRING10").alias("is_spring10"),
    )
)

filtered = (
    valid
    .filter(~F.col("is_spring10"))
    .select(
        "id",
        "customer_id",
        "k",
        "value",
        "k_title",
        "is_spring10",
        F.col("is_spring10").cast("int").alias("is_spring10_flag"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w_part).alias("is_spring10_flag_bucket"),
        F.ntile(4).over(w_part_2).alias("is_spring10_flag_bucket_2"),
        F.ntile(3).over(w_part).alias("is_spring10_flag_bucket_3"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.age"), F.col("d.name"))
)

result = (
    enriched
    .select(
        "id",
        "k",
        "value",
        "k_title",
        "is_spring10",
        "is_spring10_flag",
        "is_spring10_flag_bucket_2",
        "event_seq",
        "age",
        F.when(F.col("k").rlike("^(weight_kg|device)$"), "OTHER_ZONE").when(F.col("k").rlike("^(utm_source|gift)$"), "CORE").otherwise("PRIORITY").alias("k_zone"),
    )
)
