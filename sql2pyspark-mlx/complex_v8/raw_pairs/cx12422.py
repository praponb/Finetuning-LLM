from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_holder_id_by_n_rows_f = Window.partitionBy("n_rows", "holder_id").orderBy("n_rows", "holder_id", "start_date_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_holder_id = Window.partitionBy("n_rows", "holder_id")
w_n_rows_holder_id_by_n_rows = Window.partitionBy("n_rows", "holder_id").orderBy("n_rows", "holder_id", "start_date_year")

valid_policys = (
    df_policies
    .filter(F.col("status").isin("LAPSED", "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

qualified = (
    valid_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.month(F.col("c.reported_at")).isin(1, 2)) & (F.col("c.adjuster_id").isNotNull()), "left_semi")
    .filter(F.col("end_date") < F.lit("2026-06-18").cast("date"))
)

stats = (
    qualified
    .groupBy("holder_id", F.year("start_date").alias("start_date_year"))
    .agg(
        F.count("*").alias("n_rows"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.max("end_date").alias("last_end_date"),
        F.min("premium").alias("min_premium"),
        F.sum("premium").alias("total_premium"),
    )
)

screened = (
    stats.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date") < F.lit("2026-07-11").cast("date")), "left_anti")
    .filter(F.col("start_date_year").between(2024, 2025))
)

sequenced = (
    screened
    .select(
        "holder_id",
        "start_date_year",
        "n_rows",
        "median_premium",
        "last_end_date",
        "min_premium",
        "total_premium",
        F.first("n_rows").over(w_n_rows_holder_id_by_n_rows_f).alias("first_n_rows"),
        F.count(F.lit(1)).over(w_n_rows_holder_id).alias("rows_in_n_rows_holder_id"),
        F.lead("min_premium", 3, 0).over(w_n_rows_holder_id_by_n_rows).alias("next3_min_premium"),
        F.round(F.col("median_premium") / F.nullif(F.sum("median_premium").over(w_n_rows_holder_id), F.lit(0)), 4).alias("median_premium_share_of_n_rows_holder_id"),
        F.max("median_premium").over(w_n_rows_holder_id).alias("n_rows_holder_id_max_median_premium"),
        F.ntile(4).over(w_n_rows_holder_id_by_n_rows).alias("n_rows_quartile"),
    )
)

exploded_detail = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "HOME"), "left")
    .select(
        "s.holder_id",
        "s.start_date_year",
        "s.n_rows",
        "s.median_premium",
        "s.last_end_date",
        "s.first_n_rows",
        "s.rows_in_n_rows_holder_id",
        "s.next3_min_premium",
        "s.n_rows_holder_id_max_median_premium",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
    )
)

policy_product_stats = (
    exploded_detail
    .groupBy("policy_product")
    .agg(F.max("start_date_year").alias("max_start_date_year"))
)

above_average = (
    exploded_detail.alias("s")
    .join(policy_product_stats.alias("g"), F.col("g.policy_product") == F.col("s.policy_product"), "inner")
    .filter(F.col("s.start_date_year") == F.col("g.max_start_date_year"))
    .select(
        "s.*",
        "g.max_start_date_year",
        F.round(F.col("s.start_date_year") - F.col("g.max_start_date_year"), 2).alias("start_date_year_minus_max"),
    )
)

result = (
    above_average
    .select(
        "holder_id",
        "start_date_year",
        "n_rows",
        "median_premium",
        "last_end_date",
        "first_n_rows",
        "next3_min_premium",
        "n_rows_holder_id_max_median_premium",
        "policy_product",
        "policy_status",
        "max_start_date_year",
        "start_date_year_minus_max",
        F.initcap(F.trim(F.col("policy_product"))).alias("policy_product_clean"),
        F.date_format(F.col("last_end_date"), "yyyy-MM-dd").alias("last_end_date_label"),
    )
)
