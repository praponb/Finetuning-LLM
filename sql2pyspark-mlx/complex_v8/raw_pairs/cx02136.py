from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_group = Window.partitionBy("status_group")
w_all_by_start_date_moving_1 = Window.orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)

prepared = (
    df_policies
    .filter(F.col("premium") > 2289.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("status").isin("CANCELLED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

qualified = (
    with_details.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") >= F.lit("2024-07-19").cast("date")) & (~F.col("p.status").isin("CANCELLED", "ACTIVE")), "left_anti")
)

windowed = (
    qualified
    .select(
        "*",
        F.min("holder_risk_score").over(w_status_group).alias("status_group_min_holder_risk_score"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_group), 2).alias("premium_vs_status_group_avg"),
        F.sum("holder_risk_score").over(w_all_by_start_date_moving_1).alias("holder_risk_score_sum_2"),
    )
)

screened = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on").isNotNull()), "left_semi")
)

result = (
    screened
    .filter(F.col("product").isin("HEALTH", "LIFE", "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status_group",
        "holder_segment",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
    .orderBy(F.col("product_ref").desc())
)
