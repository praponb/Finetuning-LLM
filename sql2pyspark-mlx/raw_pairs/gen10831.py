from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-17").cast("date")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("device")).alias("device_title"),
        F.coalesce("country", F.lit("NONE")).alias("country_filled"),
    )
)

metrics = (
    base
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
    )
)

cleaned = (
    metrics
    .filter(F.col("started_at") >= "2024-02-01")
    .select(
        "*",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.when(F.col("device").isin(["tablet", "desktop"]), "Priority").otherwise("Other").alias("device_group"),
    )
)

cleaned_sessions = (
    cleaned
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "first_known_browser",
        "started_at_label",
        "device_group",
        F.trim("device").alias("clean_device"),
        F.lower("device").alias("device_lower"),
    )
)

kept_keys = (
    cleaned_sessions
    .select("customer_id")
    .exceptAll(
        df_sessions
        .filter(F.to_date("started_at") >= "2024-08-01")
        .select("customer_id")
    )
)

reconciled = (
    cleaned_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.started_at_label",
        "a.clean_device",
        F.col("b.city"),
        F.col("b.country"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.credit_limit"), F.col("b.age"))
)

wide = (
    enriched
    .groupBy("city")
    .pivot("country", ["JP", "UK", "SG"])
    .agg(F.avg("credit_limit").alias("total"))
)

result = (
    wide
    .select("city")
    .orderBy(F.col("city").desc_nulls_first())
    .limit(10)
)
