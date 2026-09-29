from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_zone_b = Window.partitionBy(F.month("commissioned_on"), "zone").orderBy("commissioned_on", "id")
w_all_by_commissioned_on_full = Window.orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_commissioned_on_year_b = Window.partitionBy(F.year("first_commissioned_on")).orderBy("prev_commissioned_on", "id")
w_first_commissioned_on_year_b_2 = Window.partitionBy(F.year("first_commissioned_on")).orderBy("prev_commissioned_on")
w_prev_commissioned_on_year_zo = Window.partitionBy(F.year("prev_commissioned_on"), "zone").orderBy("commissioned_on", "id")
w_commissioned_on_dense_rank_p = Window.partitionBy("commissioned_on_dense_rank", "prev_commissioned_on_rank").orderBy("prev_commissioned_on", "id")
w_commissioned_on_month = Window.partitionBy(F.month("commissioned_on"))

prepared = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.lag("commissioned_on").over(w_commissioned_on_month_zone_b).alias("prev_commissioned_on"),
        F.first("commissioned_on").over(w_all_by_commissioned_on_full).alias("first_commissioned_on"),
    )
)

ranked = (
    with_trends
    .select(
        "id",
        "site_name",
        "commissioned_on",
        "tariff_ref",
        "prev_commissioned_on",
        "first_commissioned_on",
        F.rank().over(w_first_commissioned_on_year_b).alias("prev_commissioned_on_rank"),
        F.rank().over(w_first_commissioned_on_year_b_2).alias("prev_commissioned_on_rank_2"),
        F.dense_rank().over(w_prev_commissioned_on_year_zo).alias("commissioned_on_dense_rank"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "site_name",
        "commissioned_on",
        "tariff_ref",
        "prev_commissioned_on",
        "first_commissioned_on",
        "prev_commissioned_on_rank",
        "prev_commissioned_on_rank_2",
        "commissioned_on_dense_rank",
        F.lead("first_commissioned_on", 2).over(w_commissioned_on_dense_rank_p).alias("next2_first_commissioned_on"),
        F.round(F.col("prev_commissioned_on_rank_2") / F.nullif(F.sum("prev_commissioned_on_rank_2").over(w_commissioned_on_month), F.lit(0)), 4).alias("prev_commissioned_on_rank_2_share_of_commissione"),
        F.row_number().over(w_commissioned_on_dense_rank_p).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "prev_commissioned_on_rank",
        "seq_no",
        "prev_commissioned_on_rank_2",
        "commissioned_on_dense_rank",
    )
    .unpivot(
        ids=["id"],
        values=["prev_commissioned_on_rank", "seq_no", "prev_commissioned_on_rank_2", "commissioned_on_dense_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
