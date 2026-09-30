from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_lower_wh_code_like_l1_n_up = Window.partitionBy("n_lower_wh_code_like_l1", "n_upper_trim_wh_code_like_c")
w_all_by_max_capacity = Window.orderBy("max_capacity", "opened_on_month")

base_warehouses = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%k1%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("capacity") >= 11506, "HIGH").when(F.col("capacity") >= 4912, "MEDIUM").otherwise("LOW").alias("capacity_tier"),
        F.date_format(F.col("opened_on"), "yyyy-MM-dd").alias("opened_on_label"),
    )
)

rollup_base = (
    base_warehouses
    .filter(F.lower(F.col("wh_code")).like("%nx%"))
    .groupBy(F.date_trunc("month", "opened_on").alias("opened_on_month"))
    .agg(
        F.sum(F.when(F.lower(F.col("wh_code")).like("%l1%"), 1).otherwise(0)).alias("n_lower_wh_code_like_l1"),
        F.sum(F.when(F.col("city").isin("Singapore", "Kuala Lumpur"), F.col("capacity"))).alias("capacity_city_in_singapore_kuala_lump"),
        F.sum(F.when(F.col("city").isNotNull(), 1).otherwise(0)).alias("n_city_is_not_null"),
        F.sum(F.when(F.upper(F.trim(F.col("wh_code"))).like("C%"), 1).otherwise(0)).alias("n_upper_trim_wh_code_like_c"),
        F.max("capacity").alias("max_capacity"),
        F.sum("capacity").alias("total_capacity"),
    )
    .filter(F.col("max_capacity") > 4912)
)

filtered = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("total_capacity").alias("overall_avg_total_capacity")))
    .filter(F.col("total_capacity") > F.col("overall_avg_total_capacity"))
    .drop("overall_avg_total_capacity")
)

metrics = (
    filtered
    .select(
        "*",
        F.round(F.col("n_upper_trim_wh_code_like_c") / F.nullif(F.sum("n_upper_trim_wh_code_like_c").over(w_n_lower_wh_code_like_l1_n_up), F.lit(0)), 4).alias("n_upper_trim_wh_code_like_c_share_of_n_lower_wh_"),
        F.count(F.lit(1)).over(w_n_lower_wh_code_like_l1_n_up).alias("rows_in_n_lower_wh_code_like_l1_n_upper_trim_wh_"),
        F.round(F.percent_rank().over(w_all_by_max_capacity), 3).alias("max_capacity_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "opened_on_month",
        "capacity_city_in_singapore_kuala_lump",
        "n_city_is_not_null",
        "n_upper_trim_wh_code_like_c",
        "max_capacity",
        "total_capacity",
        "rows_in_n_lower_wh_code_like_l1_n_upper_trim_wh_",
        "max_capacity_percent_rank",
        F.date_format(F.col("opened_on_month"), "yyyy-MM-dd HH:00").alias("opened_on_month_label"),
    )
    .orderBy(
        F.col("opened_on_month").asc_nulls_last(),
        F.col("max_capacity_percent_rank").desc_nulls_first(),
    )
)
