from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium_adj").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium_adj").asc_nulls_last(), "id")
w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy(F.col("premium_adj").desc(), "id")
w_holder_id_status_by_premium_2 = Window.partitionBy("holder_id", "status").orderBy(F.col("premium_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_prior_1 = Window.orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_moving_3 = Window.orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_holder_region_by_end_date_ru = Window.partitionBy("holder_region").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_by_end_date = Window.partitionBy("holder_region").orderBy("end_date", "id")
w_status_holder_id_by_premium_3 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium_adj").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_end_date_year = Window.partitionBy("holder_region", F.year("end_date"))
w_holder_id_status_by_premium_3 = Window.partitionBy("holder_id", "status").orderBy(F.col("premium_adj").desc())
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc_nulls_first())

filtered = (
    df_policies
    .filter((F.col("holder_id") >= 9) & (F.col("start_date") >= F.lit("2024-12-03").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.when(F.col("status").isin("LAPSED", "ACTIVE"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.premium_adj",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.nth_value("holder_joined_on", 2).over(w_status_holder_id_by_premium).alias("second_holder_joined_on"),
        (F.col("premium_adj") - F.lag("premium_adj").over(w_status_holder_id_by_premium_2)).alias("premium_adj_change"),
        F.lag("premium_adj").over(w_holder_id_status_by_premium).alias("prev_premium_adj"),
        F.first("premium_adj", ignorenulls=True).over(w_holder_id_status_by_premium_2).alias("first_known_premium_adj"),
        F.round(F.avg("premium_adj").over(w_all_by_end_date_prior_1), 2).alias("premium_adj_avg_1_prior"),
        F.nth_value("holder_joined_on", 2).over(w_all_by_end_date_full).alias("second_holder_joined_on_2"),
        F.round(F.percent_rank().over(w_holder_id_status_by_premium), 3).alias("premium_adj_percent_rank"),
        F.sum("premium_adj").over(w_all_by_end_date_moving_3).alias("premium_adj_sum_4"),
        F.sum("premium_adj").over(w_holder_id_status_by_premium_2).alias("running_premium_adj"),
        F.last("premium_adj", ignorenulls=True).over(w_holder_region_by_end_date_ru).alias("last_known_premium_adj"),
        F.round(F.cume_dist().over(w_holder_id_status_by_premium), 3).alias("premium_adj_cume_dist"),
        F.lag("end_date", 3).over(w_holder_region_by_end_date).alias("prev3_end_date"),
        F.first("holder_joined_on", ignorenulls=True).over(w_status_holder_id_by_premium_3).alias("first_known_holder_joined_on"),
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_holder_region_end_date_year), F.lit(0)), 4).alias("premium_adj_share_of_holder_region_end_date_year"),
        F.round(F.cume_dist().over(w_holder_id_status_by_premium_3), 3).alias("premium_adj_cume_dist_2"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank"),
    )
)

enriched = (
    ranked
    .filter(F.col("prev3_end_date") >= F.lit("2025-10-12").cast("date"))
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "second_holder_joined_on",
        "premium_adj_change",
        "prev_premium_adj",
        "first_known_premium_adj",
        "second_holder_joined_on_2",
        "running_premium_adj",
        "last_known_premium_adj",
        "premium_adj_cume_dist",
        "prev3_end_date",
        "first_known_holder_joined_on",
        "end_date_percent_rank",
        F.quarter(F.col("prev3_end_date")).alias("prev3_end_date_quarter"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.coalesce(F.col("running_premium_adj"), F.lit(0.0)).alias("running_premium_adj_filled"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "premium_adj_change",
        "prev_premium_adj",
        "second_holder_joined_on_2",
        "premium_adj_cume_dist",
        "prev3_end_date",
        "first_known_holder_joined_on",
        "prev3_end_date_quarter",
        "running_premium_adj_filled",
    )
)
