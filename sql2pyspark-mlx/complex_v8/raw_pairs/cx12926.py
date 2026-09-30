from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone_or_unknown", "tariff").orderBy(F.col("days_since_commissioned_on").desc(), F.col("id").desc())

prepared = (
    df_sites
    .filter(F.col("commissioned_on").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.substring(F.trim(F.col("zone")), 1, 2).alias("zone_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)

deduped = (
    prepared
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .rollup("zone_or_unknown", "tariff", "zone")
    .agg(
        F.sum(F.when(F.col("tariff").isNotNull(), 1).otherwise(0)).alias("n_tariff_is_not_null"),
    )
    .orderBy(
        F.col("zone_or_unknown").asc_nulls_last(),
        F.col("tariff").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
    )
)
