from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_product = Window.partitionBy(F.date_trunc("month", "start_date"), "product")
w_all_by_premium_adj_full = Window.orderBy(F.col("premium_adj").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_adj = Window.orderBy(F.col("premium_adj").asc_nulls_last(), "id")
w_all_by_premium_adj_prior_1 = Window.orderBy(F.col("premium_adj").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_max_distinct_holder_id_in_st = Window.partitionBy("max_distinct_holder_id_in_start_date_month_produ").orderBy(F.col("max_distinct_holder_id_in_start_date_month_produ").desc(), "holder_id")
w_max_distinct_holder_id_in_st_2 = Window.partitionBy("max_distinct_holder_id_in_start_date_month_produ").orderBy(F.col("max_distinct_holder_id_in_start_date_month_produ").desc(), "holder_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_max_distinct_holder_id_in_st_3 = Window.partitionBy("max_distinct_holder_id_in_start_date_month_produ", "holder_id")
w_max_distinct_holder_id_in_st_4 = Window.partitionBy("max_distinct_holder_id_in_start_date_month_produ").orderBy(F.col("max_distinct_holder_id_in_start_date_month_produ").desc())

filtered_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-07-07").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
    )
)

ranked = (
    filtered_policys
    .select(
        "*",
        F.max("premium_adj").over(w_start_date_month_product).alias("start_date_month_product_max_premium_adj"),
        F.nth_value("start_date", 2).over(w_all_by_premium_adj_full).alias("second_start_date"),
        F.size(F.collect_set("holder_id").over(w_start_date_month_product)).alias("distinct_holder_id_in_start_date_month_product"),
        F.round(F.percent_rank().over(w_all_by_premium_adj), 3).alias("premium_adj_percent_rank"),
        F.max("premium").over(w_all_by_premium_adj_prior_1).alias("premium_max_1_prior"),
    )
)

stats = (
    ranked
    .groupBy("holder_id")
    .agg(
        F.min("premium_max_1_prior").alias("min_premium_max_1_prior"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.max("distinct_holder_id_in_start_date_month_product").alias("max_distinct_holder_id_in_start_date_month_produ"),
    )
)

windowed = (
    stats
    .select(
        "*",
        (F.col("min_premium_max_1_prior") - F.lag("min_premium_max_1_prior").over(w_max_distinct_holder_id_in_st)).alias("min_premium_max_1_prior_change"),
        F.last("min_premium_max_1_prior").over(w_max_distinct_holder_id_in_st_2).alias("last_min_premium_max_1_prior"),
        F.min("min_premium_max_1_prior").over(w_max_distinct_holder_id_in_st_3).alias("max_distinct_holder_id_in_start_date_month_produ_2"),
        F.round(F.percent_rank().over(w_max_distinct_holder_id_in_st_4), 3).alias("max_distinct_holder_id_in_start_date_month_produ_3"),
        F.count(F.lit(1)).over(w_max_distinct_holder_id_in_st_3).alias("rows_in_max_distinct_holder_id_in_start_date_mon"),
        F.first("min_premium_max_1_prior").over(w_max_distinct_holder_id_in_st_2).alias("first_min_premium_max_1_prior"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product").isin("HOME", "AUTO")), "left_anti")
)

result = (
    without_matches
    .filter(F.col("first_min_premium_max_1_prior").between(1680.0, 9820.0))
    .select(
        "holder_id",
        "min_premium_max_1_prior",
        "max_distinct_holder_id_in_start_date_month_produ",
        "min_premium_max_1_prior_change",
        "last_min_premium_max_1_prior",
        "max_distinct_holder_id_in_start_date_month_produ_2",
        "max_distinct_holder_id_in_start_date_month_produ_3",
    )
    .orderBy(
        F.col("min_premium_max_1_prior_change").asc(),
        "last_min_premium_max_1_prior",
    )
)
