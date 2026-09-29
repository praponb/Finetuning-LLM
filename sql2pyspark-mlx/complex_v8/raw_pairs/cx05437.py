from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_status = Window.partitionBy("product", "status")
w_holder_id_status_by_days_sta = Window.partitionBy("holder_id", "status").orderBy(F.col("days_start_date_to_end_date").desc(), "id")

valid = (
    df_policies
    .filter((F.col("premium") > 1042.8) & (F.col("product").isin("HEALTH", "TRAVEL", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.nth_value("start_date", 2).over(w_holder_id_by_start_date_full).alias("second_start_date"),
        F.size(F.collect_set("holder_id").over(w_product_status)).alias("distinct_holder_id_in_product_status"),
        F.row_number().over(w_holder_id_status_by_days_sta).alias("seq_no"),
        F.round(F.col("days_start_date_to_end_date") / F.nullif(F.sum("days_start_date_to_end_date").over(w_product_status), F.lit(0)), 4).alias("days_start_date_to_end_date_share_of_product_sta"),
        F.last("start_date").over(w_holder_id_by_start_date_full).alias("last_start_date"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
        F.max("start_date").alias("last_policy_start_date"),
    )
)

profiled = (
    windowed.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
        "p.last_policy_start_date",
    )
)

derived = (
    profiled
    .filter(F.col("product") == "LIFE")
    .select(
        "*",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

result = (
    derived
    .rollup("distinct_holder_id_in_product_status", "policy_count_2", "policy_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("end_date").alias("first_end_date"),
        F.countDistinct("product").alias("distinct_products"),
    )
)
