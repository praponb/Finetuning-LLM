from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

joined = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.browser",
        "src.country",
        F.col("b.city"),
        F.col("b.age"),
        F.col("b.country").alias("customer_country"),
    )
)

with_last = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"))
)

filtered = (
    with_last
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "city",
        "page",
        F.length("city").alias("city_length"),
        F.initcap(F.trim("customer_country")).alias("customer_country_title"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

cleaned = (
    filtered
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "browser",
        "city",
        "page",
        "customer_country_title",
        "resolved_browser",
        F.coalesce("city_length", F.lit(0)).alias("city_length_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "browser",
        "customer_country_title",
        "resolved_browser",
        F.when(F.col("page").isin(["/search", "/product"]), "Tier 1").otherwise("Other").alias("page_group"),
    )
)

totals = (
    base
    .groupBy("browser")
    .agg(
        F.countDistinct("browser").alias("distinct_browsers"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
    )
)

result = (
    totals
    .select(
        "browser",
        "distinct_browsers",
        "firefox_rows",
        "browser_list",
        "safari_count",
        F.when(F.col("browser").rlike("^(Chrome|Firefox)$"), "OTHER_ZONE").when(F.col("browser").rlike("^(Safari|Edge)$"), "CORE").otherwise("EXTENDED").alias("browser_zone"),
    )
    .orderBy(F.col("distinct_browsers").desc_nulls_last())
)
