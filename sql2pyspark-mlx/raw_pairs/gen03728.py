from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_anti")
)

matched_keys = (
    kept
    .select("status", "channel")
    .exceptAll(
        df_orders
        .filter(F.year("order_date") >= 2023)
        .select("status", "channel")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.customer_id",
        "src.order_date",
        "src.discount",
        F.col("d.credit_limit"),
        F.col("d.country"),
    )
)

base = (
    enriched
    .filter((F.col("order_date") >= F.lit("2023-05-21").cast("date")) & F.col("discount").between(0.17, 0.2))
    .withColumn("country_group", F.when(F.col("country").isin(["SG", "JP"]), "Tier 1").otherwise("Other"))
)

joined = (
    base
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.customer_id", "src.country", F.col("b.email"))
)

matched_keys_2 = (
    joined
    .select("customer_id")
    .subtract(
        df_orders
        .filter(F.col("channel") != "partner")
        .select("customer_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("country")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy(F.col("row_count").desc())
)
