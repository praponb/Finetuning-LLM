from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_joined_on_year = Window.partitionBy("status", F.year("holder_joined_on")).orderBy(F.col("holder_joined_on").desc_nulls_first(), "id")
w_status_by_premium = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")
w_holder_region_end_date_year = Window.partitionBy("holder_region", F.year("end_date"))
w_holder_id_by_premium_full = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_joined_on_year_2 = Window.partitionBy("status", F.year("holder_joined_on")).orderBy(F.col("holder_joined_on").desc_nulls_first())
w_status_holder_joined_on_year_3 = Window.partitionBy("status", F.year("holder_joined_on")).orderBy(F.col("holder_joined_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_running = Window.orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_premium_full = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_distinct_holder_id = Window.partitionBy("holder_id", "distinct_holder_id_in_product").orderBy("rows_in_product", "id")
w_distinct_holder_id_in_produc = Window.partitionBy("distinct_holder_id_in_product")
w_status_by_premium_vs_holder = Window.partitionBy("status").orderBy(F.col("premium_vs_holder_region_end_date_year_avg").desc(), "id")
w_holder_id_distinct_holder_id_2 = Window.partitionBy("holder_id", "distinct_holder_id_in_product").orderBy("rows_in_product")
w_status_by_premium_vs_holder_2 = Window.partitionBy("status").orderBy(F.col("premium_vs_holder_region_end_date_year_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_policys = (
    df_policies
    .filter((F.col("holder_id") <= 3) & (F.year(F.col("start_date")) == 2024))
)

with_parent_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

metrics = (
    with_parent_holder
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        F.round(F.cume_dist().over(w_status_holder_joined_on_year), 3).alias("holder_joined_on_cume_dist"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_status_by_premium)).alias("days_since_prev_end_date"),
        F.round(F.cume_dist().over(w_holder_id_by_premium), 3).alias("premium_cume_dist"),
        F.first("end_date", ignorenulls=True).over(w_holder_id_by_premium_running).alias("first_known_end_date"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.size(F.collect_set("holder_id").over(w_product)).alias("distinct_holder_id_in_product"),
        F.round(F.avg("premium").over(w_holder_region_end_date_year), 2).alias("holder_region_end_date_year_avg_premium"),
        F.nth_value("end_date", 2).over(w_holder_id_by_premium_full).alias("second_end_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_region_end_date_year), 2).alias("premium_vs_holder_region_end_date_year_avg"),
        F.round(F.percent_rank().over(w_status_holder_joined_on_year_2), 3).alias("holder_joined_on_percent_rank"),
        F.nth_value("premium", 2).over(w_status_holder_joined_on_year_3).alias("second_premium"),
        F.first("end_date", ignorenulls=True).over(w_all_by_premium_running).alias("first_known_end_date_2"),
        F.min("premium").over(w_holder_region_end_date_year).alias("holder_region_end_date_year_min_premium"),
        F.nth_value("premium", 2).over(w_status_by_premium_full).alias("second_premium_2"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.cume_dist().over(w_holder_id_distinct_holder_id), 3).alias("rows_in_product_cume_dist"),
        F.dense_rank().over(w_holder_id_distinct_holder_id).alias("rows_in_product_dense_rank"),
        F.round(F.col("second_premium") - F.avg("second_premium").over(w_distinct_holder_id_in_produc), 2).alias("second_premium_vs_distinct_holder_id_in_product_"),
        F.lead("holder_joined_on_cume_dist").over(w_status_by_premium_vs_holder).alias("next_holder_joined_on_cume_dist"),
        F.dense_rank().over(w_holder_id_distinct_holder_id_2).alias("rows_in_product_dense_rank_2"),
        F.last("days_since_prev_end_date", ignorenulls=True).over(w_status_by_premium_vs_holder_2).alias("last_known_days_since_prev_end_date"),
    )
)

result = (
    with_trends
    .select("next_holder_joined_on_cume_dist", "id", "rows_in_product")
    .distinct()
    .orderBy(F.col("rows_in_product").desc(), "id")
)
