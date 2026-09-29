from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_start_date_ye = Window.partitionBy(F.month("end_date"), F.year("start_date"))
w_holder_region_by_end_date_ru = Window.partitionBy("holder_region").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_by_end_date_mo = Window.partitionBy("holder_region").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)

prepared_policys = (
    df_policies
    .filter(F.col("premium") <= 2844.1)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

with_parent = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status_slug",
        "product_ref",
        "holder_region",
        "holder_risk_score",
        F.count(F.lit(1)).over(w_end_date_month_start_date_ye).alias("rows_in_end_date_month_start_date_year"),
        F.round(F.avg("holder_risk_score").over(w_end_date_month_start_date_ye), 2).alias("end_date_month_start_date_year_avg_holder_risk_s"),
        F.first("end_date", ignorenulls=True).over(w_holder_region_by_end_date_ru).alias("first_known_end_date"),
        F.round(F.avg("premium").over(w_holder_region_by_end_date_mo), 2).alias("premium_avg_4"),
    )
)

detail_rows_claims = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status_slug",
        "s.holder_region",
        "s.rows_in_end_date_month_start_date_year",
        "s.first_known_end_date",
        "s.premium_avg_4",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
    )
)

result = (
    detail_rows_claims
    .groupingSets(
        [["holder_id", "holder_region"], ["holder_id"], []],
        "holder_id", "holder_region",
    )
    .agg(
        F.sum(F.when(F.col("holder_region") != "South", F.col("claim_claimed_amount"))).alias("claim_claimed_amount_holder_region_south"),
        F.count(F.when(F.col("holder_region").isin("West", "North"), 1)).alias("n_holder_region_in_west_north"),
    )
)
