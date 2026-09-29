from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

filtered = (
    df_sessions
    .filter(F.col("country").isin(["US", "TH", "DE"]))
    .select(
        "*",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.substring("browser", 1, 3).alias("browser_prefix"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "device", "browser", "started_at", "browser_prefix")
)

kept_keys = (
    deduped
    .select("customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isin([1, 7, 15, 16]))
        .select("customer_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.customer_id",
        "a.browser",
        "a.started_at",
        F.col("d.city"),
        F.col("d.name"),
        F.col("d.signup_date"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.min("started_at").alias("started_at"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.min("started_at").alias("started_at_2"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
    )
    .orderBy(F.col("cnt").desc())
)
