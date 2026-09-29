from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_product = Window.partitionBy(F.month("start_date"), "product")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_holder = Window.partitionBy("product", "holder_id").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_holder_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_start_date_dense_rank = Window.orderBy(F.col("start_date_dense_rank").desc_nulls_first())
w_start_date_dense_rank_start = Window.partitionBy("start_date_dense_rank", "start_date_decile")
w_all_by_start_date_dense_rank_2 = Window.orderBy(F.col("start_date_dense_rank").desc_nulls_first(), "id")

filtered = (
    df_policies
    .filter(F.col("product").isin("TRAVEL", "AUTO"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_start_date_month_product).alias("rows_in_start_date_month_product"),
        F.sum("holder_risk_score").over(w_all_by_start_date_running).alias("running_holder_risk_score"),
        F.last("premium").over(w_product_holder_id_by_holder).alias("last_premium"),
        F.lag("premium").over(w_all_by_start_date).alias("prev_premium"),
        F.nth_value("holder_risk_score", 2).over(w_all_by_start_date_full).alias("second_holder_risk_score"),
        F.dense_rank().over(w_all_by_start_date).alias("start_date_dense_rank"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_start_date_month_product), 2).alias("holder_risk_score_vs_start_date_month_product_av"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.ntile(10).over(w_all_by_start_date).alias("start_date_decile"),
        F.last("start_date", ignorenulls=True).over(w_all_by_start_date_running).alias("last_known_start_date"),
        (F.col("premium") - F.lag("premium").over(w_all_by_start_date)).alias("premium_change"),
        F.max("premium").over(w_start_date_month_product).alias("start_date_month_product_max_premium"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.max("holder_risk_score").over(w_product_holder_id_by_holder_2).alias("holder_risk_score_max_4"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w_all_by_start_date_dense_rank).alias("start_date_dense_rank_dense_rank"),
        F.sum("start_date_cume_dist").over(w_start_date_dense_rank_start).alias("start_date_dense_rank_start_date_decile_sum_star"),
        F.round(F.col("prev_premium") - F.avg("prev_premium").over(w_start_date_dense_rank_start), 2).alias("prev_premium_vs_start_date_dense_rank_start_date"),
        F.max("start_date_cume_dist").over(w_start_date_dense_rank_start).alias("start_date_dense_rank_start_date_decile_max_star"),
        F.lead("premium", 1, 0).over(w_all_by_start_date_dense_rank_2).alias("next_premium"),
        F.row_number().over(w_all_by_start_date_dense_rank_2).alias("seq_no"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "status",
        F.date_format("second_start_date", "yyyy-MM").alias("second_start_date_ym"),
    )
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.round(F.avg("holder_risk_score_vs_start_date_month_product_av"), 2).alias("avg_holder_risk_score_vs_start_date_month_produc"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("running_holder_risk_score")).otherwise(0)).alias("running_holder_risk_score_status_cancelled"),
    )
)

result = (
    grouped
    .filter(F.col("avg_holder_risk_score_vs_start_date_month_produc").between(-0.9, 0.7))
    .select(
        "start_date_month",
        "status",
        "second_start_date_ym",
        "distinct_holder_ids",
        "avg_holder_risk_score_vs_start_date_month_produc",
        "running_holder_risk_score_status_cancelled",
    )
    .orderBy(
        F.col("status").asc(),
        F.col("running_holder_risk_score_status_cancelled").desc_nulls_first(),
    )
)
