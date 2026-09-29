from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_holder_segment_by = Window.partitionBy("holder_id", "holder_segment").orderBy("start_date")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_holder_segment_by_2 = Window.partitionBy("holder_id", "holder_segment").orderBy("start_date", "id")

scoped_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("status").isin("LAPSED", "CANCELLED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

with_details_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.start_date_month_start",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_windows = (
    with_details_holder
    .select(
        "*",
        F.round(F.percent_rank().over(w_holder_id_holder_segment_by), 3).alias("start_date_percent_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.dense_rank().over(w_holder_id_holder_segment_by).alias("start_date_dense_rank"),
        F.round(F.cume_dist().over(w_holder_id_holder_segment_by_2), 3).alias("start_date_cume_dist"),
    )
)

per_group = (
    with_windows
    .groupBy("holder_id")
    .agg(
        F.countDistinct("holder_segment").alias("distinct_holder_segments"),
        F.countDistinct("product").alias("distinct_products"),
        F.datediff(F.max("start_date_month_start"), F.min("start_date_month_start")).alias("start_date_month_start_span_days"),
        F.min("start_date_percent_rank").alias("min_start_date_percent_rank"),
        F.round(F.avg("start_date_cume_dist"), 2).alias("avg_start_date_cume_dist"),
        F.max("start_date_percent_rank").alias("max_start_date_percent_rank_check"),
    )
    .filter(F.col("max_start_date_percent_rank_check") > 0.3)
    .drop("max_start_date_percent_rank_check")
)

result = (
    per_group
    .select(
        "holder_id",
        "start_date_month_start_span_days",
        "min_start_date_percent_rank",
        "avg_start_date_cume_dist",
    )
    .orderBy(F.col("min_start_date_percent_rank").desc())
)
