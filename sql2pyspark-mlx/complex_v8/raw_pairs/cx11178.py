from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_valid_from_ties = Window.orderBy("valid_from")
w_all_by_valid_from = Window.orderBy("valid_from", "id")
w_valid_from_to_date_tariff = Window.partitionBy(F.to_date("valid_from"), "tariff")

cleaned = (
    df_tariff_windows
    .filter(F.col("tariff").isin("FLAT", "TOU"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.initcap(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.rank().over(w_all_by_valid_from_ties).alias("valid_from_rank"),
        F.rank().over(w_all_by_valid_from).alias("valid_from_rank_2"),
        F.sum("rate").over(w_valid_from_to_date_tariff).alias("valid_from_to_date_tariff_sum_rate"),
    )
)

tagged = (
    ranked
    .filter(F.col("valid_from_rank") <= 9)
    .withColumn("tariff_ref", F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")))
)

result = (
    tagged
    .select(
        "id",
        "tariff",
        "rate",
        "tariff_clean",
        "valid_from_rank",
        "valid_from_rank_2",
        "valid_from_to_date_tariff_sum_rate",
        "tariff_ref",
        F.date_format(F.col("valid_from"), "yyyy-MM-dd").alias("valid_from_label"),
    )
    .orderBy(F.col("tariff").asc(), F.col("tariff_ref").asc())
)
