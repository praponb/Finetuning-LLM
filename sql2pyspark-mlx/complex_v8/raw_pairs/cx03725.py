from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_month_by_valid_to = Window.partitionBy(F.date_trunc("month", "valid_to")).orderBy(F.col("valid_to").desc(), "id")
w_all_by_valid_to_ties = Window.orderBy("valid_to")

prepared = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "tariff",
        "tariff_ref",
        F.ntile(4).over(w_valid_to_month_by_valid_to).alias("valid_to_quartile"),
        F.dense_rank().over(w_all_by_valid_to_ties).alias("valid_to_dense_rank"),
    )
)

result = (
    windowed
    .groupBy("valid_to_dense_rank")
    .pivot("tariff", ["TOU", "FLAT"])
    .agg(F.sum("valid_to_quartile"))
    .orderBy("valid_to_dense_rank")
)
