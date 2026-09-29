from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_start_month = Window.partitionBy(F.month("start_date_month_start"))
w_holder_id_by_start_date_mont = Window.partitionBy("holder_id").orderBy("start_date_month_start", "id")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_start_date_month_start_month_2 = Window.partitionBy(F.date_trunc("month", "start_date_month_start")).orderBy(F.col("premium").desc(), "id")

base = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

screened = (
    base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date") < F.lit("2026-12-05").cast("date")) & (F.col("p.product") == "HEALTH"), "left_anti")
)

ranked = (
    screened
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "start_date_month_start",
        F.sum("premium").over(w_start_date_month_start_month).alias("start_date_month_start_month_sum_premium"),
        F.size(F.collect_set("holder_id").over(w_start_date_month_start_month)).alias("distinct_holder_id_in_start_date_month_start_mon"),
        F.round(F.cume_dist().over(w_holder_id_by_start_date_mont), 3).alias("start_date_month_start_cume_dist"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_by_premium)).alias("premium_change"),
        F.ntile(10).over(w_start_date_month_start_month_2).alias("premium_decile"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

profiled = (
    ranked.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_date", "c.last_claim_date")
)

result = (
    profiled
    .groupingSets(
        [["distinct_holder_id_in_start_date_month_start_mon", "premium_decile"], ["distinct_holder_id_in_start_date_month_start_mon"]],
        "distinct_holder_id_in_start_date_month_start_mon", "premium_decile",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.sum("start_date_month_start_cume_dist").alias("total_start_date_month_start_cume_dist"),
        F.percentile_approx("distinct_holder_id_in_start_date_month_start_mon", 0.5).alias("median_distinct_holder_id_in_start_date_month_st"),
    )
)
