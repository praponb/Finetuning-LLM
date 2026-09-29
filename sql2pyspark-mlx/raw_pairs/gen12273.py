from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy(F.col("customer_credit_limit").desc_nulls_last())
w2 = Window.partitionBy("customer_id")

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

with_dims = (
    lines
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "a.customer_id",
        "a.product_id",
        "a.promo",
        F.col("b.credit_limit"),
        F.col("b.email"),
        F.col("b.country"),
    )
)

remaining_keys = (
    with_dims
    .select("customer_id")
    .exceptAll(
        df_carts
        .filter(F.size("coupon_codes") > 2)
        .select("customer_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("customer_id")
    )
)

verified_carts = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims_carts = (
    verified_carts
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.country") == "JP")
    .select(
        "src.customer_id",
        "src.credit_limit",
        "src.country",
        F.col("b.segment"),
        F.col("b.name"),
        F.col("b.credit_limit").alias("customer_credit_limit"),
    )
)

joined = (
    with_dims_carts
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.customer_id",
        "e.credit_limit",
        "e.name",
        "e.customer_credit_limit",
        F.col("c.signup_date"),
        F.col("c.country").alias("customer_country"),
        F.col("c.city"),
    )
)

scored = (
    joined
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("customer_credit_limit_cume_dist"),
        F.cume_dist().over(w_hist).alias("customer_credit_limit_cume_dist_2"),
        F.round((F.col("customer_credit_limit") / F.sum("customer_credit_limit").over(w2)), 4).alias("customer_credit_limit_share"),
    )
)

totals = (
    scored
    .groupBy(
        F.when(F.col("customer_credit_limit_share") >= 0.63, "LARGE").otherwise("SMALL").alias("customer_credit_limit_share_tier"),
    )
    .agg(
        F.percentile_approx("customer_credit_limit_share", 0.5).alias("customer_credit_limit_share_p50"),
        F.count(F.when(F.col("customer_country") == "JP", 1)).alias("jp_rows"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
    )
)

result = (
    totals
    .select(
        "customer_credit_limit_share_tier",
        "customer_credit_limit_share_p50",
        "jp_rows",
    )
    .orderBy(F.col("customer_credit_limit_share_tier").desc(), "jp_rows")
)
