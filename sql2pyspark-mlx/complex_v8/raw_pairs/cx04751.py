from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_dayofweek_n = Window.partitionBy("commissioned_on_dayofweek", "n_rows").orderBy(F.col("commissioned_on_dayofweek_to_installed_kw_ratio").desc())
w_commissioned_on_dayofweek_n_2 = Window.partitionBy("commissioned_on_dayofweek", "n_rows").orderBy(F.col("commissioned_on_dayofweek_to_installed_kw_ratio").desc(), "tariff", "commissioned_on_dayofweek")

scoped_sites = (
    df_sites
    .filter((F.col("tariff").isin("TOU", "EV")) & (F.col("installed_kw") < 386.6))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
        F.dayofweek(F.col("commissioned_on")).alias("commissioned_on_dayofweek"),
    )
)

stats = (
    scoped_sites
    .groupBy("tariff", "commissioned_on_dayofweek")
    .agg(
        F.sum("days_since_commissioned_on").alias("total_days_since_commissioned_on"),
        F.round(F.sum("commissioned_on_dayofweek") / F.nullif(F.sum("installed_kw"), F.lit(0)), 3).alias("commissioned_on_dayofweek_to_installed_kw_ratio"),
        F.count("*").alias("n_rows"),
        F.countDistinct("zone").alias("distinct_zones"),
    )
)

sequenced = (
    stats
    .withColumn("commissioned_on_dayofweek_to_installed_kw_ratio_", F.dense_rank().over(w_commissioned_on_dayofweek_n))
    .withColumn("next_distinct_zones", F.lead("distinct_zones").over(w_commissioned_on_dayofweek_n_2))
)

derived = (
    sequenced
    .select(
        "*",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.greatest(F.col("n_rows"), F.col("distinct_zones")).alias("greatest_n_rows_distinct_zones"),
    )
)

result = (
    derived
    .select(
        "tariff",
        "commissioned_on_dayofweek_to_installed_kw_ratio",
        "distinct_zones",
        "commissioned_on_dayofweek_to_installed_kw_ratio_",
        "next_distinct_zones",
        "tariff_len",
        "greatest_n_rows_distinct_zones",
        F.greatest(F.col("next_distinct_zones"), F.col("tariff_len")).alias("greatest_next_distinct_zones_tariff_len"),
        F.coalesce(F.col("next_distinct_zones"), F.lit(0)).alias("next_distinct_zones_filled"),
    )
    .orderBy(
        F.col("commissioned_on_dayofweek_to_installed_kw_ratio").desc_nulls_first(),
        F.col("next_distinct_zones_filled").desc(),
    )
)
