from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc_nulls_first(), "id")
w_status_by_start_date = Window.partitionBy("status").orderBy("start_date", "id")
w_status_product = Window.partitionBy("status", "product")
w_status_by_premium_ties = Window.partitionBy("status").orderBy(F.col("premium").desc())
w_status_by_premium_running = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_premium = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_products_by_n_produ = Window.partitionBy("distinct_products").orderBy("n_product_in_health_home")
w_distinct_products_n_product = Window.partitionBy("distinct_products", "n_product_in_health_home")
w_distinct_products_by_n_produ_2 = Window.partitionBy("distinct_products").orderBy("n_product_in_health_home", "second_start_date_ym")

scoped_policys = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2026)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

metrics = (
    scoped_policys
    .select(
        "*",
        F.row_number().over(w_status_holder_id_by_premium).alias("rn"),
        F.rank().over(w_status_by_start_date).alias("start_date_rank"),
        F.min("premium").over(w_status_product).alias("status_product_min_premium"),
        F.round(F.cume_dist().over(w_status_by_premium_ties), 3).alias("premium_cume_dist"),
        F.last("start_date", ignorenulls=True).over(w_status_by_premium_running).alias("last_known_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_product), F.lit(0)), 4).alias("premium_share_of_holder_id_product"),
        F.last("premium", ignorenulls=True).over(w_status_holder_id_by_premium_2).alias("last_known_premium"),
        F.lag("premium", 2).over(w_status_by_premium).alias("prev2_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product), 2).alias("premium_vs_holder_id_product_avg"),
        F.round(F.percent_rank().over(w_status_by_start_date), 3).alias("start_date_percent_rank"),
        F.nth_value("start_date", 2).over(w_all_by_premium_full).alias("second_start_date"),
    )
)

per_group = (
    metrics
    .groupBy(
        F.date_format("second_start_date", "yyyy-MM").alias("second_start_date_ym"),
    )
    .agg(
        F.round(F.sum("premium_cume_dist"), 2).alias("total_premium_cume_dist"),
        F.round(F.avg("start_date_rank"), 2).alias("avg_start_date_rank"),
        F.countDistinct("product").alias("distinct_products"),
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.sum(F.when(F.col("product").isin("HEALTH", "HOME"), 1).otherwise(0)).alias("n_product_in_health_home"),
    )
)

with_windows = (
    per_group
    .select(
        "second_start_date_ym",
        "total_premium_cume_dist",
        "avg_start_date_rank",
        "pct_status_is_not_null",
        "n_product_in_health_home",
        F.rank().over(w_distinct_products_by_n_produ).alias("n_product_in_health_home_rank"),
        F.min("avg_start_date_rank").over(w_distinct_products_n_product).alias("distinct_products_n_product_in_health_home_min_a"),
        F.count(F.lit(1)).over(w_distinct_products_n_product).alias("rows_in_distinct_products_n_product_in_health_ho"),
        F.round(F.cume_dist().over(w_distinct_products_by_n_produ_2), 3).alias("n_product_in_health_home_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "second_start_date_ym",
        "total_premium_cume_dist",
        "avg_start_date_rank",
        "pct_status_is_not_null",
        "n_product_in_health_home",
        "n_product_in_health_home_rank",
        "distinct_products_n_product_in_health_home_min_a",
        "rows_in_distinct_products_n_product_in_health_ho",
    )
    .orderBy(F.col("pct_status_is_not_null").asc_nulls_last())
)
