from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_valid_from_month_by_v = Window.partitionBy("tariff", F.date_trunc("month", "valid_from")).orderBy("valid_from", "id")
w_tariff_valid_from_to_date_by = Window.partitionBy("tariff", F.to_date("valid_from")).orderBy("valid_to", "id")
w_prev3_valid_from_month_valid = Window.partitionBy(F.month("prev3_valid_from"), F.year("valid_from"))
w_all_by_prev3_valid_from_ties = Window.orderBy("prev3_valid_from")

cleaned_tariffs = (
    df_tariff_windows
    .filter(F.month(F.col("valid_from")).isin(1, 2))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.round(F.col("rate") * 1.5, 2).alias("rate_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
    )
)

sequenced = (
    cleaned_tariffs
    .select(
        "id",
        "valid_from",
        "rate_adj",
        "days_since_valid_to",
        F.lag("valid_from", 3).over(w_tariff_valid_from_month_by_v).alias("prev3_valid_from"),
        (F.col("rate") - F.lag("rate").over(w_tariff_valid_from_to_date_by)).alias("rate_change"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.max("rate_adj").over(w_prev3_valid_from_month_valid).alias("prev3_valid_from_month_valid_from_year_max_rate_"),
        F.round(F.col("days_since_valid_to") - F.avg("days_since_valid_to").over(w_prev3_valid_from_month_valid), 2).alias("days_since_valid_to_vs_prev3_valid_from_month_va"),
        F.rank().over(w_all_by_prev3_valid_from_ties).alias("prev3_valid_from_rank"),
    )
)

scored = (
    metrics
    .filter(F.col("prev3_valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .withColumn("rate_change_filled", F.coalesce(F.col("rate_change"), F.lit(0.0)))
)

result = (
    scored
    .select(
        "id",
        "valid_from",
        "rate_adj",
        "rate_change",
        "days_since_valid_to_vs_prev3_valid_from_month_va",
        "rate_change_filled",
        F.round(F.col("rate_change_filled") * 0.25, 2).alias("rate_change_filled_adj"),
    )
)
