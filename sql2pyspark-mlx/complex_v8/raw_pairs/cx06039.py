from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_status_holder_region = Window.partitionBy("status", "holder_region")
w_holder_id_holder_segment_by = Window.partitionBy("holder_id", "holder_segment").orderBy("premium", "id")
w_status_by_premium_adj = Window.partitionBy("status").orderBy(F.col("premium_adj").asc_nulls_last(), "id")
w_holder_id_holder_segment_by_2 = Window.partitionBy("holder_id", "holder_segment").orderBy("premium_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_segment = Window.partitionBy("status", "holder_segment")

prepared_policys = (
    df_policies
    .filter(F.col("product") == "LIFE")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
    )
)

joined = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.premium_adj",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "premium",
        "premium_adj",
        F.row_number().over(w_all_by_premium).alias("rn"),
        F.size(F.collect_set("holder_id").over(w_status_holder_region)).alias("distinct_holder_id_in_status_holder_region"),
        F.round(F.cume_dist().over(w_holder_id_holder_segment_by), 3).alias("premium_cume_dist"),
        F.row_number().over(w_holder_id_holder_segment_by).alias("seq_no"),
        F.lag("premium_adj", 3).over(w_status_by_premium_adj).alias("prev3_premium_adj"),
        F.min("premium_adj").over(w_holder_id_holder_segment_by_2).alias("min_premium_adj_so_far"),
        F.max("premium").over(w_status_holder_region).alias("status_holder_region_max_premium"),
        F.size(F.collect_set("holder_id").over(w_status_holder_segment)).alias("distinct_holder_id_in_status_holder_segment"),
        (F.col("premium") - F.lag("premium").over(w_all_by_premium)).alias("premium_change"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_holder_region), 2).alias("premium_vs_status_holder_region_avg"),
        F.lead("premium_adj", 1, 0).over(w_status_by_premium_adj).alias("next_premium_adj"),
    )
)

enriched = (
    sequenced
    .select(
        "*",
        (F.col("seq_no") + F.col("premium_adj")).alias("seq_no_plus_premium_adj"),
    )
)

result = (
    enriched
    .select(
        "id",
        "premium",
        "distinct_holder_id_in_status_holder_region",
        "premium_cume_dist",
        "prev3_premium_adj",
        "premium_change",
        "premium_vs_status_holder_region_avg",
        "next_premium_adj",
        "seq_no_plus_premium_adj",
        (F.floor(F.col("min_premium_adj_so_far") / 10000) * 10000).alias("min_premium_adj_so_far_bucket"),
        (F.col("next_premium_adj") + F.col("seq_no")).alias("next_premium_adj_plus_seq_no"),
    )
    .orderBy(F.col("seq_no_plus_premium_adj").asc(), "premium_cume_dist")
)
