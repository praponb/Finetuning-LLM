from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("customer_id").isNotNull() & F.col("key").isin(["utm_source", "device"]))
    .select(
        "*",
        F.coalesce("key", F.lit("unassigned")).alias("key_filled"),
        F.year("created_at").alias("created_at_year"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.signup_date"), F.col("p.credit_limit"))
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("e.*", F.col("d.country"))
)

metrics = (
    joined
    .select(
        "customer_id",
        "key",
        "value",
        "key_filled",
        "created_at_year",
        "credit_limit",
        "country",
        F.sum("created_at_year").over(w).alias("customer_id_sum_created_at_year"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_30m"),
        F.count("created_at_year").over(w3).alias("created_at_year_count_2h"),
    )
)

matched_keys = (
    metrics
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("customer_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "customer_id",
        "value",
        "key_filled",
        "created_at_year",
        "credit_limit",
        "country",
        "customer_id_sum_created_at_year",
        "credit_limit_avg_30m",
    )
    .orderBy(F.col("customer_id_sum_created_at_year").desc_nulls_first())
)
