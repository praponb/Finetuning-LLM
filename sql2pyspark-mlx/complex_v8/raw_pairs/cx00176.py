from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_distanc = Window.partitionBy("holder_id").orderBy(F.col("premium_distance").desc(), "id")
w_holder_id_by_premium_distanc_2 = Window.partitionBy("holder_id").orderBy(F.col("premium_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_product_by_pre = Window.partitionBy("holder_region", "product").orderBy("premium", "id")
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_holder_id_by_premium_distanc_3 = Window.partitionBy("holder_id").orderBy("premium_distance", "id")
w_holder_region_product_by_pre_2 = Window.partitionBy("holder_region", "product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_premium_tercile_by_premium_t = Window.partitionBy("premium_tercile").orderBy(F.col("premium_tercile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_start_date_plus_12 = Window.partitionBy("status").orderBy("start_date_plus_12m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_holder_id = Window.partitionBy("holder_region", "holder_id")
w_all_by_first_known_premium_t = Window.orderBy(F.col("first_known_premium").desc())
w_premium_tercile_by_premium_t_2 = Window.partitionBy("premium_tercile").orderBy(F.col("premium_tercile").desc())

valid_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-08-27").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
    )
)

combined_details = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.lower(F.col("p.holder_name")).like("% f%")), "inner")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.lead("start_date_plus_12m", 3).over(w_holder_id_by_premium_distanc).alias("next3_start_date_plus_12m"),
        F.first("premium", ignorenulls=True).over(w_holder_id_by_premium_distanc_2).alias("first_known_premium"),
        F.ntile(3).over(w_holder_region_product_by_pre).alias("premium_tercile"),
        F.round(F.avg("premium").over(w_holder_id_status), 2).alias("holder_id_status_avg_premium"),
        F.datediff(F.col("start_date_plus_12m"), F.lag("start_date_plus_12m").over(w_holder_id_by_premium_distanc_3)).alias("days_since_prev_start_date_plus_12m"),
        F.count(F.lit(1)).over(w_holder_id_status).alias("rows_in_holder_id_status"),
        F.nth_value("premium", 2).over(w_holder_region_product_by_pre_2).alias("second_premium"),
        F.round(F.avg("premium_distance").over(w_holder_id), 2).alias("holder_id_avg_premium_distance"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        F.add_months(F.col("holder_joined_on"), 3).alias("holder_joined_on_plus_3m"),
        F.concat_ws(": ", F.col("holder_region"), F.col("product")).alias("holder_region_product_label"),
    )
)

sequenced = (
    annotated
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date_plus_12m",
        "premium_distance",
        "holder_joined_on",
        "holder_region",
        "premium_tercile",
        "holder_id_status_avg_premium",
        "days_since_prev_start_date_plus_12m",
        "rows_in_holder_id_status",
        "second_premium",
        "holder_joined_on_plus_3m",
        F.max("premium_distance").over(w_premium_tercile_by_premium_t).alias("max_premium_distance_so_far"),
        F.min("second_premium").over(w_status_by_start_date_plus_12).alias("min_second_premium_so_far"),
        F.round(F.col("rows_in_holder_id_status") / F.nullif(F.sum("rows_in_holder_id_status").over(w_holder_region_holder_id), F.lit(0)), 4).alias("rows_in_holder_id_status_share_of_holder_region_"),
        F.round(F.percent_rank().over(w_all_by_first_known_premium_t), 3).alias("first_known_premium_percent_rank"),
        F.rank().over(w_premium_tercile_by_premium_t_2).alias("premium_tercile_rank"),
        F.size(F.collect_set("status").over(w_holder_region_holder_id)).alias("distinct_status_in_holder_region_holder_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date_plus_12m",
        "premium_distance",
        "holder_joined_on",
        "holder_region",
        "premium_tercile",
        "holder_id_status_avg_premium",
        "days_since_prev_start_date_plus_12m",
        "rows_in_holder_id_status",
        "second_premium",
        "max_premium_distance_so_far",
        "min_second_premium_so_far",
        "rows_in_holder_id_status_share_of_holder_region_",
        "first_known_premium_percent_rank",
        "premium_tercile_rank",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)
