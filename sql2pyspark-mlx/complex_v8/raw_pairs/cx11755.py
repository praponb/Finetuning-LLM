from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_product_by_en = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("end_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_end_date_month_product_by_en_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("end_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_product_by_en_3 = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("end_date").desc(), "id")
w_latest = Window.partitionBy("product").orderBy(F.col("premium").desc(), F.col("id").desc())
w_product_by_premium_sum_3_tie = Window.partitionBy("product").orderBy("premium_sum_3")
w_product_by_premium_sum_3 = Window.partitionBy("product").orderBy("premium_sum_3", "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_product_by_premium_sum_3_pri = Window.partitionBy("product").orderBy("premium_sum_3", "id").rowsBetween(-6, -1)

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") <= 4) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

metrics = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        F.round(F.avg("premium").over(w_end_date_month_product_by_en), 2).alias("premium_avg_2"),
        F.sum("premium").over(w_end_date_month_product_by_en_2).alias("premium_sum_3"),
        F.max("premium").over(w_product_holder_id_by_premium).alias("max_premium_so_far"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_end_date_month_product_by_en_3)).alias("days_since_prev_end_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_holder_id), 2).alias("premium_vs_product_holder_id_avg"),
    )
)

latest_per_key = (
    metrics
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.claim_type").isNotNull()), "left_anti")
)

filtered = (
    qualified.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2026-12-05").cast("date"))), "left_anti")
)

with_windows = (
    filtered
    .select(
        "id",
        "holder_id",
        "premium",
        "premium_avg_2",
        "max_premium_so_far",
        "days_since_prev_end_date",
        F.rank().over(w_product_by_premium_sum_3_tie).alias("premium_sum_3_rank"),
        (F.col("max_premium_so_far") - F.lag("max_premium_so_far").over(w_product_by_premium_sum_3)).alias("max_premium_so_far_change"),
        F.sum("premium_sum_3").over(w_holder_id_product).alias("holder_id_product_sum_premium_sum_3"),
        F.min("max_premium_so_far").over(w_holder_id_product).alias("holder_id_product_min_max_premium_so_far"),
        F.sum("max_premium_so_far").over(w_product_by_premium_sum_3_pri).alias("max_premium_so_far_sum_6_prior"),
        F.sum("days_since_prev_end_date").over(w_holder_id_product).alias("holder_id_product_sum_days_since_prev_end_date"),
    )
)

enriched = (
    with_windows.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment").isNotNull()), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_avg_2",
        "s.max_premium_so_far",
        "s.premium_sum_3_rank",
        "s.max_premium_so_far_sum_6_prior",
        "s.holder_id_product_sum_days_since_prev_end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "premium_avg_2",
        "max_premium_so_far",
        "premium_sum_3_rank",
        "max_premium_so_far_sum_6_prior",
        "holder_id_product_sum_days_since_prev_end_date",
        "holder_risk_score",
        "holder_region",
    )
    .orderBy(
        F.col("holder_id").desc(),
        F.col("holder_risk_score").desc_nulls_first(),
    )
)
