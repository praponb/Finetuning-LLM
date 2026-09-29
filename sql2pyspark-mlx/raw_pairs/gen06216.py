from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

valid = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Chrome"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.when(F.col("browser").isin(["Edge", "Firefox"]), "Core").otherwise("Other").alias("browser_group"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

kept_keys = (
    valid
    .select("browser")
    .intersect(
        df_sessions
        .filter(F.col("browser") == "Chrome")
        .select("browser")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.age"), F.col("c.name"))
)

joined_sessions = (
    joined
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.email"))
)

filtered = (
    joined_sessions
    .filter(F.col("browser").eqNullSafe(F.lit("Edge")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "browser_group",
        "country_clean",
        "age",
        "name",
        "email",
        F.when(F.col("device") == "desktop", 1).otherwise(0).alias("is_desktop"),
    )
)

result = (
    filtered
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "browser_group",
        "age",
        "name",
        "is_desktop",
        F.when(F.col("country").rlike("^(DE|JP)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("is_desktop").asc_nulls_first())
)
