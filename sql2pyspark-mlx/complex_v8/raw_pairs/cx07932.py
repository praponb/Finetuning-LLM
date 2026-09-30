from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff").orderBy(F.col("rate").desc(), F.col("id").desc())

prepared = (
    df_tariff_windows
    .filter((F.col("tariff") != "TOU") & (F.month(F.col("valid_from")).isin(2, 3)))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

deduped = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

tariff_stats = (
    deduped
    .groupBy("tariff")
    .agg(F.avg("rate").alias("avg_rate"))
)

above_average = (
    deduped.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        F.col("rate_minus_avg").alias("m_rate_minus_avg"),
        F.col("rate").alias("rate_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_rate_minus_avg", "rate_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
