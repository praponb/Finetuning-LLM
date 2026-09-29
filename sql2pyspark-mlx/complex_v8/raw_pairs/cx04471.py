from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_holder_joined_on_year_holder = Window.partitionBy(F.year("holder_joined_on"), "holder_id").orderBy("premium")
w_product = Window.partitionBy("product")
w_holder_joined_on_year_holder_2 = Window.partitionBy(F.year("holder_joined_on"), "holder_id").orderBy("premium", "id")
w_product_by_premium_running = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_joined_on_runn = Window.orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_premium_ties = Window.partitionBy("product").orderBy("premium")
w_premium_rank_by_seq_no_ties = Window.partitionBy("premium_rank").orderBy(F.col("seq_no").desc())
w_holder_id = Window.partitionBy("holder_id")
w_first_known_holder_joined_on = Window.partitionBy(F.date_trunc("month", "first_known_holder_joined_on")).orderBy("premium_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_premium_rank_by_seq_no_prior = Window.partitionBy("premium_rank").orderBy(F.col("seq_no").desc(), "id").rowsBetween(-1, -1)
w_first_known_holder_joined_on_2 = Window.partitionBy(F.date_trunc("month", "first_known_holder_joined_on")).orderBy("premium_cume_dist", "id")

base = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

enriched_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

windowed = (
    enriched_holder
    .select(
        "*",
        F.row_number().over(w_product_by_premium).alias("seq_no"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_product_by_premium)).alias("days_since_prev_holder_joined_on"),
        F.ntile(10).over(w_product_by_premium).alias("premium_decile"),
        F.round(F.cume_dist().over(w_holder_joined_on_year_holder), 3).alias("premium_cume_dist"),
        F.min("premium").over(w_product).alias("product_min_premium"),
        F.lag("premium").over(w_holder_joined_on_year_holder_2).alias("prev_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_holder_joined_on_year_holder_2)).alias("days_since_prev_holder_joined_on_2"),
        F.first("holder_joined_on", ignorenulls=True).over(w_product_by_premium_running).alias("first_known_holder_joined_on"),
        F.sum("premium").over(w_all_by_holder_joined_on_runn).alias("running_premium"),
        F.rank().over(w_product_by_premium_ties).alias("premium_rank"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.round(F.cume_dist().over(w_premium_rank_by_seq_no_ties), 3).alias("seq_no_cume_dist"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.nth_value("premium_decile", 2).over(w_first_known_holder_joined_on).alias("second_premium_decile"),
        F.sum("premium_decile").over(w_premium_rank_by_seq_no_prior).alias("premium_decile_sum_1_prior"),
        F.round(F.cume_dist().over(w_first_known_holder_joined_on_2), 3).alias("premium_cume_dist_cume_dist"),
        F.rank().over(w_first_known_holder_joined_on_2).alias("premium_cume_dist_rank"),
        F.round(F.avg("product_min_premium").over(w_holder_id), 2).alias("holder_id_avg_product_min_premium"),
        F.lag("premium_share_of_product").over(w_first_known_holder_joined_on_2).alias("prev_premium_share_of_product"),
    )
)

rollup_base = (
    windowed_2
    .filter(F.col("days_since_prev_holder_joined_on").between(60, 130))
    .groupBy(
        "second_premium_decile",
        "holder_id",
        F.year("first_known_holder_joined_on").alias("first_known_holder_joined_on_year"),
    )
    .agg(
        F.countDistinct("product").alias("distinct_products"),
        F.min("rows_in_holder_id").alias("min_rows_in_holder_id"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("premium_cume_dist_cume_dist"), 2).alias("avg_premium_cume_dist_cume_dist"),
        F.round(F.stddev("premium_decile"), 2).alias("stddev_premium_decile"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    rollup_base
    .select(
        "second_premium_decile",
        "holder_id",
        "first_known_holder_joined_on_year",
        "distinct_products",
        "min_rows_in_holder_id",
        "n_rows",
        "avg_premium_cume_dist_cume_dist",
        "stddev_premium_decile",
        F.least(F.col("distinct_products"), F.col("n_rows")).alias("least_distinct_products_n_rows"),
        F.abs(F.col("second_premium_decile") - 6).alias("second_premium_decile_distance"),
    )
    .orderBy(
        F.col("second_premium_decile").asc(),
        F.col("first_known_holder_joined_on_year").asc_nulls_last(),
    )
)
