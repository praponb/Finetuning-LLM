from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_region = Window.partitionBy("product", "holder_region")
w_product_holder_id_by_start_d = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_year_ties = Window.orderBy(F.col("end_date_year").desc())

base = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2026-11-25").cast("date"))) & (F.col("holder_id") >= 9))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.year(F.col("end_date")).alias("end_date_year"),
    )
)

enriched = (
    base.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region") == "South"), "left")
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    enriched
    .withColumn("product_holder_region_sum_end_date_year", F.sum("end_date_year").over(w_product_holder_region))
    .withColumn("rows_in_product_holder_region", F.count(F.lit(1)).over(w_product_holder_region))
)

without_matches = (
    windowed.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "CANCELLED") & (F.col("p.start_date").between(F.lit("2024-01-25").cast("date"), F.lit("2025-10-13").cast("date"))), "left_anti")
    .filter(F.col("end_date_year") <= 2026)
)

with_trends = (
    without_matches
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "holder_name",
        "holder_region",
        "holder_risk_score",
        "product_holder_region_sum_end_date_year",
        "rows_in_product_holder_region",
        F.sum("premium").over(w_product_holder_id_by_start_d).alias("running_premium"),
        F.round(F.cume_dist().over(w_all_by_end_date_year_ties), 3).alias("end_date_year_cume_dist"),
    )
)

screened = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (~F.col("p.region").isin("South", "East")), "left_anti")
)

with_children_claims = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.holder_name",
        "s.holder_region",
        "s.holder_risk_score",
        "s.product_holder_region_sum_end_date_year",
        "s.rows_in_product_holder_region",
        "s.running_premium",
        "s.end_date_year_cume_dist",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_type",
    )
)

result = (
    with_children_claims
    .select(
        "id",
        "claim_id",
        F.col("rows_in_product_holder_region").alias("rows_in_product_holder_region_v"),
        F.col("product_holder_region_sum_end_date_year").alias("product_holder_region_sum_end_date_year_v"),
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["rows_in_product_holder_region_v", "product_holder_region_sum_end_date_year_v"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
