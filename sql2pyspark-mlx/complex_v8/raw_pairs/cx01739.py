from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_start_date = Window.partitionBy("status").orderBy("start_date", "id")
w_status_by_start_date_full = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_start_date_ties = Window.partitionBy("status").orderBy("start_date")
w_start_date_tercile_product = Window.partitionBy("start_date_tercile", "product")
w_holder_id_by_start_date_cume = Window.partitionBy("holder_id").orderBy(F.col("start_date_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = (
    df_policies
    .filter((F.col("status") != "LAPSED") | (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

joined_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.status_slug",
        F.col("p.segment").alias("holder_segment"),
    )
)

filtered = (
    joined_holder.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
    .filter(F.col("status").isNotNull())
)

ranked = (
    filtered
    .select(
        "*",
        F.lead("start_date").over(w_status_by_start_date).alias("next_start_date"),
        F.last("start_date").over(w_status_by_start_date_full).alias("last_start_date"),
        F.round(F.cume_dist().over(w_status_by_start_date_ties), 3).alias("start_date_cume_dist"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_by_start_date)).alias("days_since_prev_start_date"),
        F.ntile(3).over(w_status_by_start_date).alias("start_date_tercile"),
    )
)

labeled = (
    ranked
    .select(
        "*",
        F.coalesce(F.col("start_date_cume_dist"), F.lit(0.0)).alias("start_date_cume_dist_filled"),
        F.coalesce(F.col("holder_segment"), F.lit("UNKNOWN")).alias("holder_segment_or_unknown"),
        F.coalesce(F.col("start_date_cume_dist"), F.lit(0.0)).alias("start_date_cume_dist_filled_2"),
    )
)

with_trends = (
    labeled
    .select(
        "*",
        F.sum("start_date_cume_dist_filled").over(w_start_date_tercile_product).alias("start_date_tercile_product_sum_start_date_cume_d"),
        F.nth_value("start_date_tercile", 2).over(w_holder_id_by_start_date_cume).alias("second_start_date_tercile"),
    )
)

with_parent = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score").between(0.1, 0.7))
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "status_slug",
        "holder_segment",
        "next_start_date",
        "last_start_date",
        "start_date_cume_dist",
        "days_since_prev_start_date",
        "start_date_tercile",
        "start_date_cume_dist_filled",
        "second_start_date_tercile",
        "holder_joined_on",
        "holder_risk_score",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)
