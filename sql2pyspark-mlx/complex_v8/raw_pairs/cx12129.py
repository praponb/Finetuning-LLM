from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_month_by_ho = Window.partitionBy("product", F.month("end_date")).orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-3, -1)
w_holder_region = Window.partitionBy("holder_region")
w_product_by_premium_running = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_holder_region_holder_joined = Window.partitionBy("holder_region", F.date_trunc("month", "holder_joined_on")).orderBy("end_date", "id")
w_holder_region_holder_id_by_s = Window.partitionBy("holder_region", "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_end_date_month_by_ho_2 = Window.partitionBy("product", F.month("end_date")).orderBy(F.col("holder_joined_on").desc())
w_product_by_holder_joined_on = Window.partitionBy("product").orderBy(F.col("holder_joined_on_rank").desc(), "id")
w_start_date_month_end_date_mo = Window.partitionBy(F.date_trunc("month", "start_date"), F.date_trunc("month", "end_date")).orderBy("holder_joined_on", "id").rowsBetween(-3, Window.currentRow)
w_start_date_month_end_date_mo_2 = Window.partitionBy(F.date_trunc("month", "start_date"), F.date_trunc("month", "end_date")).orderBy("holder_joined_on", "id")

recent_policys = (
    df_policies
    .filter((F.col("premium") > 2289.0) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

with_parent = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region") == "Central")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "holder_joined_on",
        "holder_region",
        F.max("premium").over(w_product_end_date_month_by_ho).alias("premium_max_3_prior"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_region), F.lit(0)), 4).alias("premium_share_of_holder_region"),
        F.sum("premium").over(w_product_by_premium_running).alias("running_premium"),
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.sum("premium").over(w_holder_region).alias("holder_region_sum_premium"),
        F.min("premium").over(w_holder_region).alias("holder_region_min_premium"),
        F.count(F.lit(1)).over(w_holder_region).alias("rows_in_holder_region"),
        F.round(F.cume_dist().over(w_product_by_premium), 3).alias("premium_cume_dist"),
        F.lag("end_date").over(w_holder_region_holder_joined).alias("prev_end_date"),
        F.first("end_date").over(w_holder_region_holder_id_by_s).alias("first_end_date"),
        F.rank().over(w_product_end_date_month_by_ho_2).alias("holder_joined_on_rank"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.size(F.collect_set("product").over(w_holder_region)).alias("distinct_product_in_holder_region"),
        F.ntile(3).over(w_product_by_holder_joined_on).alias("holder_joined_on_rank_tercile"),
        F.round(F.avg("holder_id_sum_premium").over(w_start_date_month_end_date_mo), 2).alias("holder_id_sum_premium_avg_4"),
        (F.col("rows_in_holder_region") - F.lag("rows_in_holder_region").over(w_start_date_month_end_date_mo_2)).alias("rows_in_holder_region_change"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "holder_joined_on",
        "premium_max_3_prior",
        "premium_share_of_holder_region",
        "running_premium",
        "holder_id_sum_premium",
        "premium_share_of_holder_id",
        "holder_region_sum_premium",
        "rows_in_holder_region",
        "prev_end_date",
        "first_end_date",
        "holder_joined_on_rank",
        "distinct_product_in_holder_region",
        "holder_joined_on_rank_tercile",
        "holder_id_sum_premium_avg_4",
    )
    .orderBy(F.col("running_premium").desc())
)
