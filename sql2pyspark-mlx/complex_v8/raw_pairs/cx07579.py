from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year = Window.partitionBy(F.year("end_date"))
w_start_date_month_by_premium = Window.partitionBy(F.month("start_date")).orderBy(F.col("premium").desc(), "id")
w_start_date_month_by_premium_2 = Window.partitionBy(F.month("start_date")).orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month_by_premium_3 = Window.partitionBy(F.month("start_date")).orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_policys = (
    df_policies
    .filter(F.col("product") != "HEALTH")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

with_details = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status_prefix",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_trends = (
    with_details
    .select(
        "*",
        F.min("premium").over(w_end_date_year).alias("end_date_year_min_premium"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_start_date_month_by_premium)).alias("days_since_prev_end_date"),
        F.nth_value("end_date", 2).over(w_start_date_month_by_premium_2).alias("second_end_date"),
        F.first("premium", ignorenulls=True).over(w_start_date_month_by_premium_3).alias("first_known_premium"),
    )
)

with_parent_holder = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") > 0.2)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.holder_segment",
        "s.end_date_year_min_premium",
        "s.days_since_prev_end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_parent = (
    with_parent_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.region").alias("holder_region"), "p.holder_name")
)

screened = (
    with_parent.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
)

result = (
    screened
    .groupBy("holder_id")
    .pivot("product", ["LIFE", "HEALTH", "HOME", "TRAVEL"])
    .agg(F.min("premium"))
)
