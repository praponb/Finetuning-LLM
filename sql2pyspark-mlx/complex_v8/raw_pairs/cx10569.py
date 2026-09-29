from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_holder_id_by_s = Window.partitionBy("holder_region", "holder_id").orderBy(F.col("start_date").desc_nulls_first(), "id")
w_product_group_start_date_mon = Window.partitionBy("product_group", F.month("start_date"))
w_status_group_by_end_date_run = Window.partitionBy("status_group").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_policys = (
    df_policies
    .filter((F.col("status") != "CANCELLED") | (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("LIFE", "AUTO"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
    )
)

derived = (
    base_policys
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "product_group",
        F.when(F.col("status").isin("ACTIVE", "LAPSED"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
        F.concat(F.col("product_group"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_group_ref"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.product_group",
        "s.status_group",
        "s.product_group_ref",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "holder_id",
        "start_date",
        "status_group",
        "product_group_ref",
        "holder_name",
        "holder_risk_score",
        "holder_region",
        F.rank().over(w_holder_region_holder_id_by_s).alias("start_date_rank"),
        F.size(F.collect_set("holder_region").over(w_product_group_start_date_mon)).alias("distinct_holder_region_in_product_group_start_da"),
        F.last("holder_risk_score", ignorenulls=True).over(w_status_group_by_end_date_run).alias("last_known_holder_risk_score"),
    )
)

enriched = (
    ranked
    .select(
        "id",
        "start_date",
        "product_group_ref",
        "holder_name",
        "last_known_holder_risk_score",
        F.round(F.col("start_date_rank") / F.nullif(F.col("holder_risk_score"), F.lit(0)), 3).alias("start_date_rank_per_holder_risk_score"),
        F.coalesce(F.col("holder_risk_score"), F.lit(0.0)).alias("holder_risk_score_filled"),
    )
)

result = (
    enriched
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "start_date",
        "holder_name",
        "start_date_rank_per_holder_risk_score",
        "holder_risk_score_filled",
        F.round(F.col("holder_risk_score_filled") * 0.9, 2).alias("holder_risk_score_filled_adj"),
        F.length(F.trim(F.col("product_group_ref"))).alias("product_group_ref_len"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
