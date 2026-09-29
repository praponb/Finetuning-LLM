from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

valid = df_meter_readings.filter(F.col("voltage") >= 218.2)

deduped = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    deduped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    enriched
    .select(
        "id",
        F.col("kwh").alias("m_kwh"),
        F.col("site_installed_kw").alias("site_installed_kw_v"),
        F.col("voltage").alias("m_voltage"),
    )
    .unpivot(
        ids=["id"],
        values=["m_kwh", "site_installed_kw_v", "m_voltage"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
