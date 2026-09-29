from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

remaining_keys = (
    exploded
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.size("coupon_codes") > 1)
        .select("customer_id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.id", "a.customer_id", "a.promo", F.col("p.city"), F.col("p.signup_date"))
)

enriched = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.signup_date") < "2024-05-21")
    .select(
        "src.customer_id",
        "src.signup_date",
        F.col("p.country"),
        F.col("p.email"),
        F.col("p.age"),
    )
)

result = (
    enriched
    .groupBy(F.when(F.col("age") >= 70, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
    )
    .orderBy(F.col("avg_age").desc_nulls_last())
)
