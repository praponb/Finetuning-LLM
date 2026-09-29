from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_start_date_month_by_premium = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("premium", "id").rowsBetween(-6, Window.currentRow)
w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy("end_date_plus_365d")
w_holder_id_by_premium_filled = Window.partitionBy("holder_id").orderBy("premium_filled", "id")
w_status_holder_id_by_end_date_2 = Window.partitionBy("status", "holder_id").orderBy("end_date_plus_365d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_status = Window.partitionBy(F.year("start_date"), "status")

base_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.date_add(F.col("end_date"), 365).alias("end_date_plus_365d"),
    )
)

with_windows = (
    base_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "premium_filled",
        "end_date_plus_365d",
        F.sum("premium").over(w_product).alias("product_sum_premium"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.col("premium_filled") / F.nullif(F.sum("premium_filled").over(w_product), F.lit(0)), 4).alias("premium_filled_share_of_product"),
        F.sum("premium_filled").over(w_start_date_month_by_premium).alias("premium_filled_sum_7"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.round(F.cume_dist().over(w_status_holder_id_by_end_date), 3).alias("end_date_plus_365d_cume_dist"),
        F.lead("premium_filled", 1, 0).over(w_holder_id_by_premium_filled).alias("next_premium_filled"),
        F.last("end_date_plus_365d").over(w_status_holder_id_by_end_date_2).alias("last_end_date_plus_365d"),
        F.count(F.lit(1)).over(w_start_date_year_status).alias("rows_in_start_date_year_status"),
    )
)

result = (
    ranked
    .groupingSets(
        [["holder_id", "status"], ["holder_id"], []],
        "holder_id", "status",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(100 * F.count(F.when(F.col("status").isin("CANCELLED", "LAPSED", "EXPIRED"), 1)) / F.count("*"), 1).alias("pct_status_in_cancelled_lapsed_e"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "gid",
        "pct_status_in_cancelled_lapsed_e",
    )
)
