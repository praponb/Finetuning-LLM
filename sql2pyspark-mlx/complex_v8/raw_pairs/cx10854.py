from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_start_date_mo = Window.partitionBy(F.date_trunc("month", "end_date"), F.month("start_date")).orderBy(F.col("start_date_month_start").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_start_date_mo_2 = Window.partitionBy(F.date_trunc("month", "end_date"), F.month("start_date")).orderBy(F.col("start_date_month_start").desc_nulls_first())
w_product_end_date_year_by_sta = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("start_date_month_start_cume_dist").asc_nulls_last())
w_product_end_date_year_by_sta_2 = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("start_date_month_start_cume_dist").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_count_end_date_month = Window.partitionBy("claim_count", F.date_trunc("month", "end_date"))

filtered_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("end_date") < F.lit("2026-06-18").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

windowed = (
    filtered_policys
    .select(
        "*",
        F.first("start_date_month_start", ignorenulls=True).over(w_end_date_month_start_date_mo).alias("first_known_start_date_month_start"),
        F.last("start_date", ignorenulls=True).over(w_end_date_month_start_date_mo).alias("last_known_start_date"),
        F.round(F.cume_dist().over(w_end_date_month_start_date_mo_2), 3).alias("start_date_month_start_cume_dist"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.rank().over(w_product_end_date_year_by_sta).alias("start_date_month_start_cume_dist_rank"),
        F.first("claim_count").over(w_product_end_date_year_by_sta_2).alias("first_claim_count"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_claim_count_end_date_month), F.lit(0)), 4).alias("claim_count_share_of_claim_count_end_date_month"),
        F.dense_rank().over(w_product_end_date_year_by_sta).alias("start_date_month_start_cume_dist_dense_rank"),
    )
)

result = (
    with_trends
    .groupBy("start_date")
    .pivot("status", ["EXPIRED", "CANCELLED", "LAPSED", "ACTIVE"])
    .agg(F.sum("start_date_month_start_cume_dist"))
)
