from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc(), "id")
w_status = Window.partitionBy("status")
w_status_by_start_date_prior_6 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-6, -1)
w_all_by_premium = Window.orderBy("premium", "id")
w_product_by_start_date_prior = Window.partitionBy("product").orderBy("start_date", "id").rowsBetween(-6, -1)
w_status_by_start_date_full = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product = Window.partitionBy("product")
w_product_by_start_date_full = Window.partitionBy("product").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_ties = Window.orderBy("premium")
w_holder_id_distinct_holder_id = Window.partitionBy("holder_id", "distinct_holder_id_in_status")
w_premium_quintile_by_distinct = Window.partitionBy("premium_quintile").orderBy("distinct_holder_id_in_status", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-03-21").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.rank().over(w_holder_id_product_by_premium).alias("premium_rank"),
        F.size(F.collect_set("holder_id").over(w_status)).alias("distinct_holder_id_in_status"),
        F.min("premium").over(w_status).alias("status_min_premium"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.sum("premium").over(w_status_by_start_date_prior_6).alias("premium_sum_6_prior"),
        F.round(F.cume_dist().over(w_all_by_premium), 3).alias("premium_cume_dist"),
        F.sum("premium").over(w_product_by_start_date_prior).alias("premium_sum_6_prior_2"),
        F.ntile(5).over(w_holder_id_product_by_premium).alias("premium_quintile"),
        F.first("premium").over(w_status_by_start_date_full).alias("first_premium"),
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.nth_value("premium", 2).over(w_product_by_start_date_full).alias("second_premium"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank_2"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product_ref",
        "status_min_premium",
        "rows_in_status",
        "premium_sum_6_prior",
        "premium_sum_6_prior_2",
        "premium_quintile",
        "first_premium",
        "product_max_premium",
        "second_premium",
        F.sum("premium_sum_6_prior").over(w_holder_id_distinct_holder_id).alias("holder_id_distinct_holder_id_in_status_sum_premi"),
        F.first("premium_sum_6_prior_2", ignorenulls=True).over(w_premium_quintile_by_distinct).alias("first_known_premium_sum_6_prior_2"),
        F.count(F.lit(1)).over(w_holder_id_distinct_holder_id).alias("rows_in_holder_id_distinct_holder_id_in_status"),
    )
)

result = (
    ranked
    .filter(F.col("premium_sum_6_prior").between(3900.0, 12040.0))
    .select(
        "id",
        "holder_id",
        "status_min_premium",
        "rows_in_status",
        "premium_sum_6_prior",
        "premium_sum_6_prior_2",
        "premium_quintile",
        "first_premium",
        "product_max_premium",
        "second_premium",
        "holder_id_distinct_holder_id_in_status_sum_premi",
        "first_known_premium_sum_6_prior_2",
        "rows_in_holder_id_distinct_holder_id_in_status",
        F.substring(F.trim(F.col("product_ref")), 1, 4).alias("product_ref_prefix"),
    )
    .orderBy(
        "holder_id_distinct_holder_id_in_status_sum_premi",
        F.col("first_known_premium_sum_6_prior_2").desc_nulls_first(),
    )
)
