from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product_by_premium_di = Window.partitionBy("status", "product").orderBy(F.col("premium_distance_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_status_product_by_premium_di_2 = Window.partitionBy("status", "product").orderBy(F.col("premium_distance_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_product_by_premium_di_3 = Window.partitionBy("status", "product").orderBy(F.col("premium_distance_2").desc(), "id")

filtered = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("LAPSED", "CANCELLED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.abs(F.col("premium") - 3532.7).alias("premium_distance"),
        F.abs(F.col("premium") - 2945.9).alias("premium_distance_2"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.sum("premium").over(w_status_product_by_premium_di).alias("running_premium"),
        F.max("premium_distance_2").over(w_holder_id_product).alias("holder_id_product_max_premium_distance_2"),
        F.first("premium_distance_2").over(w_status_product_by_premium_di_2).alias("first_premium_distance_2"),
        F.last("end_date").over(w_status_product_by_premium_di_2).alias("last_end_date"),
        F.last("premium_distance_2", ignorenulls=True).over(w_status_product_by_premium_di).alias("last_known_premium_distance_2"),
        F.lag("premium_distance", 2).over(w_status_product_by_premium_di_3).alias("prev2_premium_distance"),
    )
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.avg("last_known_premium_distance_2").alias("overall_avg_last_known_premium_distance_2")))
    .filter(F.col("last_known_premium_distance_2") > F.col("overall_avg_last_known_premium_distance_2"))
    .drop("overall_avg_last_known_premium_distance_2")
)

combined_details_holder = (
    with_activity.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") == "VIP"), "inner")
    .filter(F.lower(F.col("p.holder_name")).like("%us%"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.premium_distance_2",
        "s.running_premium",
        "s.holder_id_product_max_premium_distance_2",
        "s.first_premium_distance_2",
        "s.last_end_date",
        "s.last_known_premium_distance_2",
        "s.prev2_premium_distance",
        "p.holder_name",
    )
)

scored = (
    combined_details_holder
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.least(F.col("premium_distance_2"), F.col("running_premium")).alias("least_premium_distance_2_running_premium"),
        F.year(F.col("last_end_date")).alias("last_end_date_year"),
    )
)

filtered_2 = (
    scored.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") > 0.2), "left_semi")
)

enriched = (
    filtered_2
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "premium_distance_2",
        "running_premium",
        "first_premium_distance_2",
        "last_end_date",
        "last_known_premium_distance_2",
        "prev2_premium_distance",
        "holder_name",
        "days_since_start_date",
        "least_premium_distance_2_running_premium",
        "last_end_date_year",
        F.substring(F.trim(F.col("holder_name")), 1, 4).alias("holder_name_prefix"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.date_add(F.col("last_end_date"), 365).alias("last_end_date_plus_365d"),
    )
)

result = (
    enriched
    .filter(F.col("prev2_premium_distance") > 1250.0)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "premium_distance_2",
        "last_known_premium_distance_2",
        "prev2_premium_distance",
        "holder_name",
        "least_premium_distance_2_running_premium",
        "last_end_date_year",
        "holder_name_prefix",
        "product_prefix",
        "last_end_date_plus_365d",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
    .orderBy(F.col("last_end_date_year").asc_nulls_last())
)
