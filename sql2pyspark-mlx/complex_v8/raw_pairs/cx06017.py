from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_product_holder_segment_by_ho = Window.partitionBy("product", "holder_segment").orderBy("holder_risk_score", "id")
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_all_by_premium = Window.orderBy(F.col("premium").desc_nulls_first(), "id")
w_all_by_premium_full = Window.orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_start_date_runn = Window.partitionBy("holder_id").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_ties = Window.orderBy(F.col("premium").desc_nulls_first())
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy(F.col("start_date").asc_nulls_last(), "id")

prepared_policys = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2026-01-23").cast("date"), F.lit("2027-03-20").cast("date"))) & (F.col("status") != "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2844.1).alias("premium_distance"),
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
    )
)

with_details_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

sequenced = (
    with_details_holder
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "premium_distance",
        "end_date_quarter",
        "holder_risk_score",
        F.round(F.avg("premium_distance").over(w_status), 2).alias("status_avg_premium_distance"),
        F.ntile(10).over(w_product_holder_segment_by_ho).alias("holder_risk_score_decile"),
        F.size(F.collect_set("status").over(w_end_date_year)).alias("distinct_status_in_end_date_year"),
        F.round(F.col("end_date_quarter") / F.nullif(F.sum("end_date_quarter").over(w_end_date_year), F.lit(0)), 4).alias("end_date_quarter_share_of_end_date_year"),
        F.lead("start_date", 3).over(w_all_by_premium).alias("next3_start_date"),
        F.last("premium_distance").over(w_all_by_premium_full).alias("last_premium_distance"),
        F.count(F.lit(1)).over(w_end_date_year).alias("rows_in_end_date_year"),
        F.last("premium_distance").over(w_holder_id_by_start_date_full).alias("last_premium_distance_2"),
        F.max("holder_risk_score").over(w_holder_id_by_start_date_runn).alias("max_holder_risk_score_so_far"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_end_date_year), 2).alias("holder_risk_score_vs_end_date_year_avg"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.lag("end_date").over(w_holder_id_by_start_date).alias("prev_end_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_year), 2).alias("premium_vs_end_date_year_avg"),
        F.row_number().over(w_holder_id_by_start_date).alias("rn"),
    )
)

derived = (
    sequenced
    .select(
        "id",
        "product",
        "status",
        "premium_distance",
        "end_date_quarter",
        "holder_risk_score",
        "status_avg_premium_distance",
        "holder_risk_score_decile",
        "distinct_status_in_end_date_year",
        "rows_in_end_date_year",
        "last_premium_distance_2",
        "max_holder_risk_score_so_far",
        "premium_percent_rank",
        "holder_risk_score_vs_end_date_year_avg",
        "premium_share_of_status",
        "prev_end_date",
        "premium_vs_end_date_year_avg",
        "rn",
        F.concat_ws("-", F.col("product"), F.col("status")).alias("product_status_label"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
    )
)

result = (
    derived
    .select(
        "id",
        "product",
        "status",
        "premium_distance",
        "holder_risk_score",
        "status_avg_premium_distance",
        "holder_risk_score_decile",
        "distinct_status_in_end_date_year",
        "rows_in_end_date_year",
        "max_holder_risk_score_so_far",
        "premium_percent_rank",
        "holder_risk_score_vs_end_date_year_avg",
        "premium_share_of_status",
        "prev_end_date",
        "premium_vs_end_date_year_avg",
        "rn",
        "product_status_label",
        "status_prefix",
        "end_date_label",
    )
    .orderBy(F.col("premium_distance").desc())
)
