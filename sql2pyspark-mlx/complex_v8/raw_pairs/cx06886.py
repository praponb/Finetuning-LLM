from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_adj_ful = Window.partitionBy("holder_id").orderBy(F.col("premium_adj").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_holder_id = Window.partitionBy(F.year("start_date"), "holder_id")
w_start_date_quarter_by_start = Window.partitionBy("start_date_quarter").orderBy("start_date", "id")
w_holder_id_by_premium_adj = Window.partitionBy("holder_id").orderBy(F.col("premium_adj").desc_nulls_first(), "id")
w_start_date_quarter_holder_id = Window.partitionBy("start_date_quarter", "holder_id")
w_holder_segment_status_by_end = Window.partitionBy("holder_segment", "status").orderBy("end_date", "id")

valid = (
    df_policies
    .filter(~F.col("status").isin("CANCELLED", "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
    )
)

with_parent_holder = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_quarter",
        "s.premium_adj",
        F.col("p.segment").alias("holder_segment"),
    )
)

sequenced = (
    with_parent_holder
    .select(
        "*",
        F.nth_value("premium_adj", 2).over(w_holder_id_by_premium_adj_ful).alias("second_premium_adj"),
        F.min("start_date_quarter").over(w_start_date_year_holder_id).alias("start_date_year_holder_id_min_start_date_quarter"),
        F.round(F.percent_rank().over(w_start_date_quarter_by_start), 3).alias("start_date_percent_rank"),
        F.ntile(5).over(w_holder_id_by_premium_adj).alias("premium_adj_quintile"),
        F.round(F.avg("start_date_quarter").over(w_start_date_quarter_holder_id), 2).alias("start_date_quarter_holder_id_avg_start_date_quar"),
        F.round(F.col("start_date_quarter") / F.nullif(F.sum("start_date_quarter").over(w_start_date_quarter_holder_id), F.lit(0)), 4).alias("start_date_quarter_share_of_start_date_quarter_h"),
        F.lag("start_date").over(w_holder_segment_status_by_end).alias("prev_start_date"),
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_start_date_year_holder_id), F.lit(0)), 4).alias("premium_adj_share_of_start_date_year_holder_id"),
        F.lead("start_date_quarter", 2).over(w_start_date_quarter_by_start).alias("next2_start_date_quarter"),
        F.count(F.lit(1)).over(w_start_date_year_holder_id).alias("rows_in_start_date_year_holder_id"),
    )
)

result = (
    sequenced
    .filter(F.col("premium_adj") < 5675.0)
    .select(
        "id",
        "start_date",
        "end_date",
        "start_date_quarter",
        "holder_segment",
        "second_premium_adj",
        "start_date_percent_rank",
        "premium_adj_quintile",
        "start_date_quarter_holder_id_avg_start_date_quar",
        "start_date_quarter_share_of_start_date_quarter_h",
        "prev_start_date",
        "next2_start_date_quarter",
        "rows_in_start_date_year_holder_id",
        (F.col("premium_adj") + F.col("next2_start_date_quarter")).alias("premium_adj_plus_next2_start_date_quarter"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)
