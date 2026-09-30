from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_group = Window.partitionBy("product_group")
w_status_by_end_date_running = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_status_by_end_date_prior_1 = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(-1, -1)
w_holder_id_product_group_by_s = Window.partitionBy("holder_id", "product_group").orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, -1)
w_status_by_end_date_moving_2 = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_holder_id_by_end_date_moving = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w_status_by_end_date = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")

recent = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-05-02").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("HEALTH", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
    )
)

enriched_holder = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_group",
        "p.holder_name",
    )
)

with_windows = (
    enriched_holder
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "product_group",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_group), F.lit(0)), 4).alias("premium_share_of_product_group"),
        F.first("start_date", ignorenulls=True).over(w_status_by_end_date_running).alias("first_known_start_date"),
        F.max("premium").over(w_product_group).alias("product_group_max_premium"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_by_end_date)).alias("premium_change"),
        F.sum("premium").over(w_status_by_end_date_prior_1).alias("premium_sum_1_prior"),
        F.round(F.avg("premium").over(w_holder_id_product_group_by_s), 2).alias("premium_avg_1_prior"),
        F.sum("premium").over(w_status_by_end_date_moving_2).alias("premium_sum_3"),
        F.max("premium").over(w_holder_id_by_end_date_moving).alias("premium_max_7"),
        F.min("premium").over(w_product_group).alias("product_group_min_premium"),
        F.round(F.percent_rank().over(w_status_by_end_date), 3).alias("end_date_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product",
        "start_date",
        "end_date",
        "product_group",
        "premium_share_of_product_group",
        "first_known_start_date",
        "product_group_max_premium",
        "premium_change",
        "premium_sum_1_prior",
        "premium_sum_3",
        "premium_max_7",
        "product_group_min_premium",
        "end_date_percent_rank",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.date_trunc("quarter", F.col("first_known_start_date")).alias("first_known_start_date_quarter"),
    )
    .orderBy("first_known_start_date", F.col("end_date_percent_rank").asc())
)
