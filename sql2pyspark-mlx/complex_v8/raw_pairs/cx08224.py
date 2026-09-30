from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_start_date_year_p = Window.partitionBy("product").orderBy("start_date_year", "id").rowsBetween(-3, -1)
w_product = Window.partitionBy("product")
w_product_by_start_date_year = Window.partitionBy("product").orderBy("start_date_year", "id")
w_product_by_start_date_year_r = Window.partitionBy("product").orderBy("start_date_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_policies
    .filter(F.col("holder_id") <= 17)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.lower(F.trim(F.col("status"))).alias("status_clean_2"),
    )
)

tagged = (
    scoped
    .select(
        "*",
        F.year(F.col("start_date")).alias("start_date_year"),
        F.date_add(F.col("end_date"), 30).alias("end_date_plus_30d"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.sum("start_date_year").over(w_product_by_start_date_year_p).alias("start_date_year_sum_3_prior"),
        F.round(F.avg("start_date_year").over(w_product), 2).alias("product_avg_start_date_year"),
        F.lag("end_date", 2).over(w_product_by_start_date_year).alias("prev2_end_date"),
        F.first("start_date", ignorenulls=True).over(w_product_by_start_date_year_r).alias("first_known_start_date"),
        F.round(F.col("start_date_year") - F.avg("start_date_year").over(w_product), 2).alias("start_date_year_vs_product_avg"),
    )
)

screened = (
    windowed
    .crossJoin(windowed.agg(F.max("premium").alias("overall_max_premium")))
    .filter(F.col("premium") < F.col("overall_max_premium"))
    .drop("overall_max_premium")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status") != "REJECTED", 1)).alias("claim_status_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

summarized_rows = (
    screened.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_reported_at",
    )
)

with_activity = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.avg("start_date_year").alias("overall_avg_start_date_year")))
    .filter(F.col("start_date_year") > F.col("overall_avg_start_date_year"))
    .drop("overall_avg_start_date_year")
)

result = (
    with_activity
    .cube("product", "status", "holder_id")
    .agg(
        F.round(F.stddev("start_date_year"), 2).alias("stddev_start_date_year"),
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
