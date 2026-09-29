from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

cleaned = (
    flattened
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.when(F.col("tag").isin(["BUNDLE", "WELCOME5"]), "Group A").otherwise("Other").alias("tag_group"),
        F.coalesce("tag", F.lit("OTHER")).alias("resolved_tag"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.city"))
)

cleaned_carts = (
    joined
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 20:00:00"))) & (F.col("created_at") >= "2023-10-01"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        "tag_group",
        "city",
        F.concat("city", F.lit("/"), "tag").alias("city_tag"),
    )
)

totals = (
    cleaned_carts
    .rollup(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes_2"),
    )
)

result = (
    totals
    .select(
        "hour_window",
        "span_minutes",
        "cnt",
        "duration_min",
        "num_tags",
        "tag_list",
        "span_minutes_2",
    )
)
