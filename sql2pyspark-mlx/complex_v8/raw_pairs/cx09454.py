from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_month_month_by_dis = Window.partitionBy(F.month("opened_on_month")).orderBy(F.col("distinct_ids").desc(), "city", "opened_on_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids_by_n_wh_code_is = Window.partitionBy("distinct_ids").orderBy(F.col("n_wh_code_is_not_null").desc(), "city", "opened_on_month").rowsBetween(-3, -1)
w_distinct_ids_by_n_wh_code_is_2 = Window.partitionBy("distinct_ids").orderBy(F.col("n_wh_code_is_not_null").desc(), "city", "opened_on_month")

base_warehouses = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("K%"))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
    )
)

per_group = (
    base_warehouses
    .groupBy("city", F.date_trunc("month", "opened_on").alias("opened_on_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("wh_code").isNotNull(), 1)).alias("n_wh_code_is_not_null"),
    )
)

sequenced = (
    per_group
    .select(
        "city",
        "opened_on_month",
        "distinct_ids",
        "n_wh_code_is_not_null",
        F.nth_value("opened_on_month", 2).over(w_opened_on_month_month_by_dis).alias("second_opened_on_month"),
        F.round(F.avg("n_wh_code_is_not_null").over(w_distinct_ids_by_n_wh_code_is), 2).alias("n_wh_code_is_not_null_avg_3_prior"),
        F.ntile(4).over(w_distinct_ids_by_n_wh_code_is_2).alias("n_wh_code_is_not_null_quartile"),
        F.dense_rank().over(w_distinct_ids_by_n_wh_code_is_2).alias("n_wh_code_is_not_null_dense_rank"),
    )
)

enriched = (
    sequenced
    .select(
        "city",
        "opened_on_month",
        "distinct_ids",
        "n_wh_code_is_not_null",
        "second_opened_on_month",
        "n_wh_code_is_not_null_avg_3_prior",
        "n_wh_code_is_not_null_dense_rank",
        F.least(F.col("distinct_ids"), F.col("n_wh_code_is_not_null")).alias("least_distinct_ids_n_wh_code_is_not_null"),
    )
)

labeled = (
    enriched
    .filter(F.col("n_wh_code_is_not_null_avg_3_prior") < 8.0)
    .select(
        "city",
        "opened_on_month",
        "distinct_ids",
        "n_wh_code_is_not_null",
        "second_opened_on_month",
        "n_wh_code_is_not_null_dense_rank",
        "least_distinct_ids_n_wh_code_is_not_null",
        F.concat(F.col("city"), F.lit("#"), F.col("distinct_ids").cast("string")).alias("city_ref"),
    )
)

enriched_2 = (
    labeled
    .filter(F.col("n_wh_code_is_not_null_dense_rank") <= 8)
    .select(
        "city",
        "opened_on_month",
        "distinct_ids",
        "n_wh_code_is_not_null",
        "second_opened_on_month",
        "n_wh_code_is_not_null_dense_rank",
        "least_distinct_ids_n_wh_code_is_not_null",
        "city_ref",
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
    )
)

derived = (
    enriched_2
    .filter(F.col("n_wh_code_is_not_null_dense_rank").between(4, 8))
    .select(
        "*",
        F.coalesce(F.col("n_wh_code_is_not_null"), F.lit(0)).alias("n_wh_code_is_not_null_filled"),
        F.concat(F.col("city_ref"), F.lit("#"), F.col("n_wh_code_is_not_null").cast("string")).alias("city_ref_ref"),
    )
)

result = (
    derived
    .cube("n_wh_code_is_not_null", "city", "n_wh_code_is_not_null_filled")
    .agg(
        F.grouping("n_wh_code_is_not_null").alias("is_total_n_wh_code_is_not_null"),
        F.sum("distinct_ids_filled").alias("total_distinct_ids_filled"),
        F.round(100 * F.count(F.when(F.col("city") == "Bangkok", 1)) / F.count("*"), 1).alias("pct_city_bangkok"),
    )
    .orderBy(
        F.col("n_wh_code_is_not_null").asc_nulls_last(),
        F.col("city").asc_nulls_last(),
        F.col("n_wh_code_is_not_null_filled").asc_nulls_last(),
    )
)
