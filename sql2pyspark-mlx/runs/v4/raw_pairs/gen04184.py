from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("coupon_codes_count").asc_nulls_first())
w2 = Window.partitionBy("customer_id", "attr_key")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

base = (
    lines
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "customer_id",
        "coupon_codes",
        "attr_key",
        "value",
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled"),
    )
)

scored = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("coupon_codes_count_percent_rank"),
        F.avg("coupon_codes_count").over(w2).alias("customer_id_avg_coupon_codes_count"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.signup_date"), F.col("p.credit_limit"), F.col("p.email"))
)

grouped = (
    with_dims
    .groupBy("attr_key")
    .agg(
        F.sum(F.when(F.col("attr_key") == "gift", F.col("coupon_codes_count_percent_rank")).otherwise(0)).alias("gift_coupon_codes_count_percent_rank"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.percentile_approx("coupon_codes_count_percent_rank", 0.9).alias("coupon_codes_count_percent_rank_p90"),
    )
)

result = (
    grouped
    .select(
        "attr_key",
        "gift_coupon_codes_count_percent_rank",
        "num_attr_keys",
        "coupon_codes_count_percent_rank_p90",
    )
)
