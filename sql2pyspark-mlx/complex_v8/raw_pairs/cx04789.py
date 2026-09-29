from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_holder_join = Window.partitionBy(F.month("start_date"), F.month("holder_joined_on"))
w_holder_id_product_by_holder = Window.partitionBy("holder_id", "product").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_product_by_holder_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id")

prepared = (
    df_policies
    .filter(~F.col("product").isin("AUTO", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

filtered = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") == "SME"), "left_semi")
)

with_details_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2025-02-22").cast("date"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

sequenced = (
    with_details_holder
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_month_holder_join), 2).alias("premium_vs_start_date_month_holder_joined_on_mon"),
        F.first("holder_joined_on").over(w_holder_id_product_by_holder).alias("first_holder_joined_on"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_start_date_month_holder_join), F.lit(0)), 4).alias("holder_risk_score_share_of_start_date_month_hold"),
        F.lag("holder_joined_on").over(w_holder_id_product_by_holder_2).alias("prev_holder_joined_on"),
        F.sum("premium").over(w_start_date_month_holder_join).alias("start_date_month_holder_joined_on_month_sum_prem"),
    )
)

result = (
    sequenced
    .cube("product", "holder_id")
    .agg(
        F.min("premium_vs_start_date_month_holder_joined_on_mon").alias("min_premium_vs_start_date_month_holder_joined_on"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "holder_id",
        "min_premium_vs_start_date_month_holder_joined_on",
    )
)
