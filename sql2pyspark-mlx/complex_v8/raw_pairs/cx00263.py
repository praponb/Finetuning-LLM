from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_status_product = Window.partitionBy("status", "product")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc(), "id")
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium = Window.orderBy("premium", "id")
w_status_end_date_month_by_pre = Window.partitionBy("status", F.date_trunc("month", "end_date")).orderBy(F.col("premium").asc_nulls_last())
w_product_by_premium_prior_1 = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(-1, -1)
w_status_end_date_month_by_pre_2 = Window.partitionBy("status", F.date_trunc("month", "end_date")).orderBy(F.col("premium").asc_nulls_last(), "id")
w_status_holder_id_by_premium_3 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc())

prepared = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-10-23").cast("date")) | (F.col("premium") < 2945.9))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.holder_name")
)

windowed = (
    enriched
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        "days_start_date_to_end_date",
        "holder_name",
        F.lag("premium", 2).over(w_product_by_premium).alias("prev2_premium"),
        F.round(F.col("days_start_date_to_end_date") / F.nullif(F.sum("days_start_date_to_end_date").over(w_status_product), F.lit(0)), 4).alias("days_start_date_to_end_date_share_of_status_prod"),
        F.size(F.collect_set("holder_id").over(w_status_product)).alias("distinct_holder_id_in_status_product"),
        F.lag("days_start_date_to_end_date", 1, 0).over(w_status_holder_id_by_premium).alias("prev_days_start_date_to_end_date"),
        F.last("days_start_date_to_end_date", ignorenulls=True).over(w_status_holder_id_by_premium_2).alias("last_known_days_start_date_to_end_date"),
        F.round(F.percent_rank().over(w_product_by_premium), 3).alias("premium_percent_rank"),
        (F.col("premium") - F.lag("premium").over(w_all_by_premium)).alias("premium_change"),
        F.round(F.percent_rank().over(w_status_end_date_month_by_pre), 3).alias("premium_percent_rank_2"),
        F.round(F.avg("premium").over(w_product_by_premium_prior_1), 2).alias("premium_avg_1_prior"),
        F.row_number().over(w_status_holder_id_by_premium).alias("rn"),
        F.row_number().over(w_all_by_premium).alias("premium_seq"),
        F.lead("end_date").over(w_status_holder_id_by_premium).alias("next_end_date"),
        F.round(F.cume_dist().over(w_status_end_date_month_by_pre_2), 3).alias("premium_cume_dist"),
        F.round(F.percent_rank().over(w_status_holder_id_by_premium_3), 3).alias("premium_percent_rank_3"),
    )
)

totals = (
    windowed
    .groupBy("distinct_holder_id_in_status_product")
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("holder_name"))) >= 5, F.col("days_start_date_to_end_date_share_of_status_prod"))).alias("days_start_date_to_end_date_share_of_status_prod_2"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

result = (
    totals
    .select(
        "distinct_holder_id_in_status_product",
        "days_start_date_to_end_date_share_of_status_prod_2",
        "distinct_status",
        F.abs(F.col("distinct_status") - 5).alias("distinct_status_distance"),
        F.coalesce(F.col("distinct_status"), F.lit(0)).alias("distinct_status_filled"),
    )
    .orderBy("distinct_status")
)
