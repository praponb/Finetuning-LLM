from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_week_full = Window.orderBy(F.col("start_date_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_week = Window.orderBy(F.col("start_date_week").desc(), "id")
w_start_date_week_month_end_da = Window.partitionBy(F.month("start_date_week"), F.year("end_date"))

base_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
        F.date_trunc("week", F.col("start_date")).alias("start_date_week"),
    )
)

without_matches = (
    base_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "EXPIRED") & (F.year(F.col("p.end_date")) == 2026), "left_anti")
    .filter(F.col("holder_id").isNotNull())
)

windowed = (
    without_matches
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "premium_bucket",
        "start_date_week",
        F.last("start_date_week").over(w_all_by_start_date_week_full).alias("last_start_date_week"),
        (F.col("premium_bucket") - F.lag("premium_bucket").over(w_all_by_start_date_week)).alias("premium_bucket_change"),
        F.round(F.col("premium_bucket") - F.avg("premium_bucket").over(w_start_date_week_month_end_da), 2).alias("premium_bucket_vs_start_date_week_month_end_date"),
    )
)

result = (
    windowed
    .groupingSets(
        [["holder_id", "product"], ["holder_id"], ["product"]],
        "holder_id", "product",
    )
    .agg(
        F.round(F.avg("premium_bucket_change"), 2).alias("avg_premium_bucket_change"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
