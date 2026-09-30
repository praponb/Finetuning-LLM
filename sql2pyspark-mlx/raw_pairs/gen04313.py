from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("country") != "US")
    .select(
        "*",
        F.concat("browser", F.lit("/"), "device").alias("browser_device"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_clean"),
    )
)

kept_keys = (
    base
    .select("browser", "id")
    .intersect(
        df_sessions
        .filter(F.col("browser").isNotNull())
        .select("browser", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.lower(F.trim(F.col("d.name"))) == "bob smith")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.country",
        "a.browser_device",
        "a.browser_clean",
        F.col("d.city"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.name"), F.col("p.country").alias("customer_country"))
)

result = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy(F.col("customer_id").desc())
)
