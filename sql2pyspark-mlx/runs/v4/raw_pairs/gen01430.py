from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(F.col("country") != "JP")
        .select("country", "browser")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("device") == "desktop")
    .withColumn("browser_lower", F.lower("browser"))
)

enriched = (
    base
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"), F.col("d.city"))
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.*",
        F.col("c.city").alias("customer_city"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.name"),
    )
)

result = (
    joined
    .select(
        "customer_id",
        "device",
        "browser",
        "started_at",
        "city",
        "customer_city",
        "customer_credit_limit",
        "name",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "GROUP_B").when(F.col("device").rlike("^(tablet)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("device_zone"),
    )
    .orderBy(F.col("device").desc_nulls_first())
)
