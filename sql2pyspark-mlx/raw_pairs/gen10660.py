from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("is_mobile", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country")

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"))
)

enriched = (
    joined
    .select(
        "id",
        "country",
        "is_mobile",
        "country_clean",
        F.avg("is_mobile").over(w).alias("is_mobile_avg_3"),
        F.count("credit_limit").over(w2).alias("country_count_credit_limit"),
    )
)

cleaned = (
    enriched
    .filter(F.col("is_mobile_avg_3").between(24, 64))
    .select(
        "id",
        "is_mobile",
        "country_clean",
        "is_mobile_avg_3",
        F.trim("country").alias("clean_country"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("is_mobile") >= 71, "LARGE").otherwise("SMALL").alias("is_mobile_tier"),
    )
    .agg(
        F.percentile_approx("is_mobile", [0.25, 0.5, 0.75]).alias("is_mobile_quartiles"),
        F.percentile_approx("is_mobile", [0.25, 0.5, 0.75]).alias("is_mobile_quartiles_2"),
    )
)
