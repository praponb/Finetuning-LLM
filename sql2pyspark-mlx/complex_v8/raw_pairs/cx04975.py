from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_valid_to_full = Window.orderBy(F.col("valid_to").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_to_date = Window.partitionBy(F.to_date("valid_from"))

recent_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") >= 4.2) & (F.month(F.col("valid_to")).isin(1, 3)))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

ranked = (
    recent_tariffs
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.nth_value("valid_to", 2).over(w_all_by_valid_to_full).alias("second_valid_to"),
        F.last("valid_from").over(w_all_by_valid_to_full).alias("last_valid_from"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_valid_from_to_date), F.lit(0)), 4).alias("rate_share_of_valid_from_to_date"),
        F.first("rate").over(w_all_by_valid_to_full).alias("first_rate"),
    )
)

result = (
    ranked
    .select("id", "rate_share_of_valid_from_to_date", "first_rate")
    .unpivot(
        ids=["id"],
        values=["rate_share_of_valid_from_to_date", "first_rate"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
