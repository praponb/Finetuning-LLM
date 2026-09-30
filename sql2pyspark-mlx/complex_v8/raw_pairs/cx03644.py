from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_by_start_dat = Window.partitionBy(F.year("start_date")).orderBy("start_date", "id")
w_start_date_month_holder_id = Window.partitionBy(F.month("start_date"), "holder_id")
w_all_by_end_date = Window.orderBy("end_date", "id")

scoped = (
    df_policies
    .filter((F.col("product").isin("LIFE", "TRAVEL", "AUTO")) & (F.col("start_date") < F.lit("2025-03-06").cast("date")))
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

annotated = (
    scoped
    .filter(F.col("holder_id").isNotNull())
    .select("*", F.abs(F.col("premium") - 2824.5).alias("premium_distance"))
)

windowed = (
    annotated
    .select(
        "*",
        (F.col("premium_distance") - F.lag("premium_distance").over(w_start_date_year_by_start_dat)).alias("premium_distance_change"),
        F.count(F.lit(1)).over(w_start_date_month_holder_id).alias("rows_in_start_date_month_holder_id"),
        F.rank().over(w_all_by_end_date).alias("end_date_rank"),
        F.ntile(4).over(w_all_by_end_date).alias("end_date_quartile"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("end_date_quartile").alias("m_end_date_quartile"),
        F.col("claim_count").alias("claim_count_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_end_date_quartile", "claim_count_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
