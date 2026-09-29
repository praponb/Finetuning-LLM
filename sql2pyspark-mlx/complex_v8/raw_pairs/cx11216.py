from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year = Window.partitionBy(F.year("end_date"))
w_all_by_start_date = Window.orderBy("start_date", "id")

prepared = (
    df_policies
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

qualified = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isNotNull()) & (F.col("c.approved_amount") >= 0.0), "left_anti")
)

windowed = (
    qualified
    .select(
        "*",
        F.count(F.lit(1)).over(w_end_date_year).alias("rows_in_end_date_year"),
        F.size(F.collect_set("product").over(w_end_date_year)).alias("distinct_product_in_end_date_year"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_start_date)).alias("days_since_prev_end_date"),
    )
)

scored = (
    windowed
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "rows_in_end_date_year",
        "distinct_product_in_end_date_year",
        F.last_day(F.col("end_date")).alias("end_date_month_end"),
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
    )
)

combined_details_holder = (
    scored.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.rows_in_end_date_year",
        "s.distinct_product_in_end_date_year",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    combined_details_holder
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "rows_in_end_date_year",
        "distinct_product_in_end_date_year",
        "holder_segment",
        "holder_risk_score",
        "holder_region",
        F.greatest(F.col("holder_risk_score"), F.col("rows_in_end_date_year")).alias("greatest_holder_risk_score_rows_in_end_date_year"),
        (F.floor(F.col("rows_in_end_date_year") / 5) * 5).alias("rows_in_end_date_year_bucket"),
    )
)
