from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_days_since_end_da = Window.partitionBy("product").orderBy(F.col("days_since_end_date").desc(), "id")
w_product_holder_id_by_days_si = Window.partitionBy("product", "holder_id").orderBy("days_since_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_days_si_2 = Window.partitionBy("product", "holder_id").orderBy("days_since_end_date", "id").rowsBetween(-6, Window.currentRow)
w_product_by_start_date_ties = Window.partitionBy("product").orderBy("start_date")
w_product_by_days_since_end_da_2 = Window.partitionBy("product").orderBy(F.col("days_since_end_date").desc())
w_end_date_month_start_date_ye = Window.partitionBy(F.date_trunc("month", "end_date"), F.year("start_date")).orderBy(F.col("start_date").asc_nulls_last(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_product_by_days_since_end_da_3 = Window.partitionBy("product").orderBy(F.col("days_since_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_days_si_3 = Window.partitionBy("product", "holder_id").orderBy("days_since_end_date", "id").rowsBetween(-3, Window.currentRow)
w_product_by_days_since_end_da_4 = Window.partitionBy("product").orderBy(F.col("days_since_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_start_date_month = Window.partitionBy("product", F.date_trunc("month", "start_date"))
w_product_distinct_product_in = Window.partitionBy("product", "distinct_product_in_holder_id")
w_all_by_days_since_end_date_d = Window.orderBy("days_since_end_date_dense_rank", "id").rowsBetween(-1, -1)
w_last_known_end_date_month_pr = Window.partitionBy(F.date_trunc("month", "last_known_end_date"), "product").orderBy(F.col("last_start_date").desc(), "id")

scoped_policys = (
    df_policies
    .filter(F.col("premium") <= 2844.1)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

with_trends = (
    scoped_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "days_since_end_date",
        F.ntile(4).over(w_product_by_days_since_end_da).alias("days_since_end_date_quartile"),
        F.last("premium", ignorenulls=True).over(w_product_holder_id_by_days_si).alias("last_known_premium"),
        F.round(F.avg("premium").over(w_product_holder_id_by_days_si_2), 2).alias("premium_avg_7"),
        F.round(F.cume_dist().over(w_product_by_start_date_ties), 3).alias("start_date_cume_dist"),
        F.rank().over(w_product_by_days_since_end_da_2).alias("days_since_end_date_rank"),
        F.dense_rank().over(w_product_by_days_since_end_da).alias("days_since_end_date_dense_rank"),
        F.lead("end_date").over(w_end_date_month_start_date_ye).alias("next_end_date"),
        F.size(F.collect_set("product").over(w_holder_id)).alias("distinct_product_in_holder_id"),
        F.round(F.col("days_since_end_date") - F.avg("days_since_end_date").over(w_holder_id), 2).alias("days_since_end_date_vs_holder_id_avg"),
        F.last("start_date").over(w_product_by_days_since_end_da_3).alias("last_start_date"),
        F.round(F.percent_rank().over(w_product_by_days_since_end_da_2), 3).alias("days_since_end_date_percent_rank"),
        F.max("days_since_end_date").over(w_product_holder_id_by_days_si_3).alias("days_since_end_date_max_4"),
        F.last("end_date", ignorenulls=True).over(w_product_by_days_since_end_da_4).alias("last_known_end_date"),
        F.size(F.collect_set("holder_id").over(w_product_start_date_month)).alias("distinct_holder_id_in_product_start_date_month"),
        F.sum("days_since_end_date").over(w_holder_id).alias("holder_id_sum_days_since_end_date"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.round(F.col("premium_avg_7") - F.avg("premium_avg_7").over(w_product_distinct_product_in), 2).alias("premium_avg_7_vs_product_distinct_product_in_hol"),
        F.max("days_since_end_date_rank").over(w_all_by_days_since_end_date_d).alias("days_since_end_date_rank_max_1_prior"),
        F.size(F.collect_set("holder_id").over(w_product_distinct_product_in)).alias("distinct_holder_id_in_product_distinct_product_i"),
        F.ntile(5).over(w_last_known_end_date_month_pr).alias("last_start_date_quintile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "days_since_end_date",
        "last_known_premium",
        "premium_avg_7",
        "start_date_cume_dist",
        "next_end_date",
        "distinct_product_in_holder_id",
        "days_since_end_date_percent_rank",
        "days_since_end_date_max_4",
        "last_known_end_date",
        "premium_avg_7_vs_product_distinct_product_in_hol",
        "days_since_end_date_rank_max_1_prior",
        F.round(F.col("start_date_cume_dist") / F.nullif(F.col("last_known_premium"), F.lit(0)), 3).alias("start_date_cume_dist_per_last_known_premium"),
    )
    .orderBy(
        F.col("last_known_end_date").desc_nulls_first(),
        F.col("premium_avg_7_vs_product_distinct_product_in_hol").desc(),
    )
)
