from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_end_date_r = Window.partitionBy("product", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_prior_2 = Window.orderBy("end_date", "id").rowsBetween(-2, -1)
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_holder_id_status_by_end_date = Window.partitionBy("holder_id", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_start_date = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w_product_holder_id_by_min_pre = Window.partitionBy("product", "holder_id").orderBy("min_premium_so_far", "id")
w_start_date_month_by_min_prem = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("min_premium_so_far").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_policies
    .filter((F.col("premium").between(918.1, 3544.1)) & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

sequenced = (
    base
    .select(
        "*",
        F.last("start_date", ignorenulls=True).over(w_product_status_by_end_date_r).alias("last_known_start_date"),
        F.max("premium").over(w_all_by_end_date_prior_2).alias("premium_max_2_prior"),
        F.round(F.avg("premium").over(w_holder_id_status), 2).alias("holder_id_status_avg_premium"),
        F.min("premium").over(w_holder_id_status_by_end_date).alias("min_premium_so_far"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
        F.sum("premium").over(w_status_by_start_date_running).alias("running_premium"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_status_by_start_date)).alias("days_since_prev_end_date"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.round(F.col("running_premium") / F.nullif(F.sum("running_premium").over(w_holder_id_status), F.lit(0)), 4).alias("running_premium_share_of_holder_id_status"),
        F.row_number().over(w_product_holder_id_by_min_pre).alias("rn"),
        F.first("premium_max_2_prior", ignorenulls=True).over(w_start_date_month_by_min_prem).alias("first_known_premium_max_2_prior"),
    )
)

stats = (
    windowed
    .filter(F.col("min_premium_so_far") >= 5180.0)
    .groupBy(
        F.year("start_date").alias("start_date_year"),
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "holder_id",
    )
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.max("running_premium_share_of_holder_id_status").alias("max_running_premium_share_of_holder_id_status"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)

result = (
    stats
    .select(
        "start_date_year",
        "holder_id",
        "distinct_holder_ids",
        "max_running_premium_share_of_holder_id_status",
        "distinct_ids",
        "product_list",
    )
    .orderBy(
        F.col("max_running_premium_share_of_holder_id_status").desc_nulls_first(),
    )
)
