from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_city_not_in_kuala_lumpur_s = Window.partitionBy("n_city_not_in_kuala_lumpur_sin").orderBy(F.col("total_capacity").desc(), "city", "opened_on_ym")
w_n_lower_wh_code_like_bk = Window.partitionBy("n_lower_wh_code_like_bk")

filtered = (
    df_warehouses
    .filter(F.col("city").isin("Kuala Lumpur", "Bangkok"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_format(F.col("opened_on"), "MMM yyyy").alias("opened_on_label"),
        F.date_format(F.col("opened_on"), "MMM yyyy").alias("opened_on_label_2"),
        F.concat_ws(": ", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
    )
)

totals = (
    filtered
    .groupBy("city", F.date_format("opened_on", "yyyy-MM").alias("opened_on_ym"))
    .agg(
        F.count(F.when(~F.col("city").isin("Kuala Lumpur", "Singapore"), 1)).alias("n_city_not_in_kuala_lumpur_sin"),
        F.min("opened_on").alias("first_opened_on"),
        F.count(F.when(F.lower(F.col("wh_code")).like("%bk%"), 1)).alias("n_lower_wh_code_like_bk"),
        F.sum("capacity").alias("total_capacity"),
    )
)

metrics = (
    totals
    .select(
        "*",
        F.lead("total_capacity").over(w_n_city_not_in_kuala_lumpur_s).alias("next_total_capacity"),
        (F.col("n_lower_wh_code_like_bk") - F.lag("n_lower_wh_code_like_bk").over(w_n_city_not_in_kuala_lumpur_s)).alias("n_lower_wh_code_like_bk_change"),
        F.min("n_lower_wh_code_like_bk").over(w_n_lower_wh_code_like_bk).alias("n_lower_wh_code_like_bk_min_n_lower_wh_code_like"),
    )
)

result = (
    metrics
    .groupingSets(
        [["n_city_not_in_kuala_lumpur_sin", "n_lower_wh_code_like_bk"], ["n_city_not_in_kuala_lumpur_sin"]],
        "n_city_not_in_kuala_lumpur_sin", "n_lower_wh_code_like_bk",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("n_rows"),
        F.countDistinct("city").alias("distinct_citys"),
    )
    .orderBy(
        F.col("n_city_not_in_kuala_lumpur_sin").asc_nulls_last(),
        F.col("n_lower_wh_code_like_bk").asc_nulls_last(),
    )
)
