from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id_by_holder = Window.partitionBy("product", "holder_id").orderBy(F.col("holder_joined_on").desc())
w_product_holder_id_by_holder_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_holder_3 = Window.partitionBy("product", "holder_id").orderBy(F.col("holder_joined_on").desc(), "id")
w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.date_trunc("month", "start_date")).orderBy("start_date", "id")
w_all_by_holder_joined_on = Window.orderBy(F.col("holder_joined_on").desc(), "id")
w_product_holder_id_by_holder_4 = Window.partitionBy("product", "holder_id").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_holder_joined_on_ties = Window.orderBy(F.col("holder_joined_on").desc())
w_holder_region = Window.partitionBy("holder_region")
w_all_by_second_start_date_ful = Window.orderBy("second_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_policys = df_policies.filter(F.col("status").isin("ACTIVE", "EXPIRED", "CANCELLED"))

with_parent_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

ranked = (
    with_parent_holder
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "holder_region",
        F.round(F.percent_rank().over(w_product_holder_id_by_holder), 3).alias("holder_joined_on_percent_rank"),
        F.first("start_date").over(w_product_holder_id_by_holder_2).alias("first_start_date"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_product_holder_id_by_holder_3)).alias("days_since_prev_holder_joined_on"),
        F.dense_rank().over(w_holder_id_start_date_month_b).alias("start_date_dense_rank"),
        F.rank().over(w_product_holder_id_by_holder).alias("holder_joined_on_rank"),
        F.ntile(3).over(w_all_by_holder_joined_on).alias("holder_joined_on_tercile"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_product_holder_id_by_holder_3)).alias("days_since_prev_start_date"),
        F.first("start_date", ignorenulls=True).over(w_product_holder_id_by_holder_4).alias("first_known_start_date"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.first("holder_joined_on").over(w_product_holder_id_by_holder_2).alias("first_holder_joined_on"),
        F.lag("start_date", 2).over(w_all_by_start_date).alias("prev2_start_date"),
        F.round(F.percent_rank().over(w_all_by_holder_joined_on_ties), 3).alias("holder_joined_on_percent_rank_2"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "holder_region",
        "holder_joined_on_percent_rank",
        "first_start_date",
        "start_date_dense_rank",
        "holder_joined_on_rank",
        "holder_joined_on_tercile",
        "first_known_start_date",
        "second_start_date",
        "first_holder_joined_on",
        "prev2_start_date",
        "holder_joined_on_percent_rank_2",
        F.size(F.collect_set("holder_id").over(w_holder_region)).alias("distinct_holder_id_in_holder_region"),
        F.nth_value("start_date_dense_rank", 2).over(w_all_by_second_start_date_ful).alias("second_start_date_dense_rank"),
        F.round(F.col("holder_joined_on_rank") / F.nullif(F.sum("holder_joined_on_rank").over(w_holder_region), F.lit(0)), 4).alias("holder_joined_on_rank_share_of_holder_region"),
        F.round(F.col("days_since_prev_start_date") - F.avg("days_since_prev_start_date").over(w_holder_region), 2).alias("days_since_prev_start_date_vs_holder_region_avg"),
    )
)

result = (
    ranked_2
    .filter(F.col("days_since_prev_start_date_vs_holder_region_avg").between(-140.0, 80.0))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "holder_region",
        "holder_joined_on_percent_rank",
        "first_start_date",
        "start_date_dense_rank",
        "holder_joined_on_rank",
        "holder_joined_on_tercile",
        "first_known_start_date",
        "second_start_date",
        "first_holder_joined_on",
        "prev2_start_date",
        "holder_joined_on_percent_rank_2",
        "distinct_holder_id_in_holder_region",
        "second_start_date_dense_rank",
        "holder_joined_on_rank_share_of_holder_region",
        "days_since_prev_start_date_vs_holder_region_avg",
        F.dayofweek(F.col("first_start_date")).alias("first_start_date_dayofweek"),
        F.dayofweek(F.col("start_date")).alias("start_date_dayofweek"),
    )
    .orderBy(
        F.col("start_date_dayofweek").asc(),
        "distinct_holder_id_in_holder_region",
    )
)
