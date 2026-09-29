from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("age_per_utm_source").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

exploded_carts = exploded.select("id", "customer_id", "k", F.explode("coupon_codes").alias("code"))

enriched = (
    exploded_carts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.age"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.signup_date"), F.col("c.age").alias("customer_age"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.country"))
)

kept_keys = (
    with_dims
    .select("customer_id", "id")
    .exceptAll(
        df_carts
        .filter(F.col("created_at") < "2024-05-25")
        .select("customer_id", "id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .groupBy(
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        "country",
        F.when(F.col("customer_age") >= 57, "PREMIUM").otherwise("STANDARD").alias("customer_age_tier"),
    )
    .agg(
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("k") == "utm_source", 1)), F.lit(0)), 2).alias("age_per_utm_source"),
        F.round(F.sum("customer_age") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("customer_age_per_th"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .withColumn("age_per_utm_source_rank", F.rank().over(w))
)

result = (
    stats
    .select(
        "quarter_start",
        "country",
        "customer_age_tier",
        "age_per_utm_source",
        "customer_age_per_th",
        "code_list",
        "uk_count",
    )
    .orderBy(F.col("customer_age_per_th").desc())
)
