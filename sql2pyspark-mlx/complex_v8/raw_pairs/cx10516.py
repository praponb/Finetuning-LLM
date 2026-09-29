from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_plus_7d_month_by_va = Window.partitionBy(F.month("valid_to_plus_7d")).orderBy(F.col("valid_to").asc_nulls_last(), "id")
w_tariff = Window.partitionBy("tariff")

prepared = (
    df_tariff_windows
    .filter(F.col("rate") < 5.9)
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        (F.col("valid_to") + F.expr("INTERVAL 7 DAYS")).alias("valid_to_plus_7d"),
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
    )
)

qualified = (
    prepared
    .crossJoin(prepared.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

without_matches = (
    qualified
    .crossJoin(qualified.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

sequenced = (
    without_matches
    .select(
        "id",
        "rate",
        "tariff_clean",
        F.lead("valid_from").over(w_valid_to_plus_7d_month_by_va).alias("next_valid_from"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.row_number().over(w_valid_to_plus_7d_month_by_va).alias("valid_to_seq"),
    )
)

result = (
    sequenced
    .filter(F.col("valid_to_seq") >= 4)
    .select(
        "id",
        "rate",
        "tariff_clean",
        "rows_in_tariff",
        "valid_to_seq",
    )
    .orderBy("id", "rows_in_tariff")
)
