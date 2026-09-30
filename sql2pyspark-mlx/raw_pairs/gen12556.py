from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.credit_limit").between(5500, 10200))
    .select("a.*", F.col("c.age"))
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.product_id",
        "a.qty",
        "a.age",
        F.col("c.credit_limit"),
    )
)

enriched_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.email"))
)

base = (
    enriched_carts
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-02 00:00:00"))) & F.col("age").between(38, 39))
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        "age",
        "credit_limit",
        "email",
        F.hour("created_at").alias("created_at_hour"),
        F.substring("email", 1, 4).alias("email_prefix"),
    )
)

grouped = (
    base
    .groupBy(
        F.when(F.col("created_at_hour") >= 74, "LARGE").otherwise("SMALL").alias("created_at_hour_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("created_at_hour", [0.25, 0.5, 0.75]).alias("created_at_hour_quartiles"),
        F.count("*").alias("event_count"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)

result = (
    grouped
    .select(
        "created_at_hour_tier",
        "row_count",
        "distinct_customer_ids",
        "num_rows",
        "event_count",
        "avg_credit_limit",
    )
    .orderBy(F.col("event_count").desc(), "row_count")
)
