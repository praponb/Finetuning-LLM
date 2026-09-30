from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), F.col("id").desc())

prepared = (
    df_tariff_windows
    .filter(~F.col("tariff").isin("TOU", "EV"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

deduped = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_activity = (
    deduped
    .crossJoin(deduped.agg(F.max("tariff_len").alias("overall_max_tariff_len")))
    .filter(F.col("tariff_len") < F.col("overall_max_tariff_len"))
    .drop("overall_max_tariff_len")
)

result = (
    with_activity
    .select(
        "id",
        "valid_from",
        "valid_to",
        "tariff_len",
    )
    .orderBy(F.col("valid_to").asc(), "tariff_len", "id")
    .limit(10)
)
