from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_plus_30d_mo = Window.partitionBy("product", F.month("end_date_plus_30d"))
w_end_date_month_by_end_date_p = Window.partitionBy(F.month("end_date")).orderBy(F.col("end_date_plus_30d").desc(), "id")
w_all_by_premium = Window.orderBy("premium", "id")

filtered_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("product").isin("AUTO", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.date_add(F.col("end_date"), 30).alias("end_date_plus_30d"),
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
    )
)

without_matches = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isNotNull()), "left_anti")
)

with_details = (
    without_matches.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.product_group",
        "s.end_date_plus_30d",
        "s.end_date_week",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

with_trends = (
    with_details
    .select(
        "*",
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_product_end_date_plus_30d_mo), F.lit(0)), 4).alias("holder_risk_score_share_of_product_end_date_plus"),
        F.row_number().over(w_end_date_month_by_end_date_p).alias("seq_no"),
        F.round(F.cume_dist().over(w_all_by_premium), 3).alias("premium_cume_dist"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["product_group", "holder_id"], ["product_group"], []],
        "product_group", "holder_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("holder_risk_score_share_of_product_end_date_plus"), 2).alias("avg_holder_risk_score_share_of_product_end_date_"),
        F.countDistinct("product_group").alias("distinct_product_groups"),
        F.count("*").alias("row_count"),
    )
)
