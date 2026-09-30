from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_joined_on_mont = Window.partitionBy("status", F.month("holder_joined_on"))
w_holder_joined_on_month_holde = Window.partitionBy(F.month("holder_joined_on"), "holder_id").orderBy("premium", "id").rowsBetween(-1, -1)
w_all_by_premium = Window.orderBy("premium", "id")
w_holder_joined_on_month_holde_2 = Window.partitionBy(F.month("holder_joined_on"), "holder_id").orderBy("premium")
w_holder_joined_on_month_holde_3 = Window.partitionBy(F.month("holder_joined_on"), "holder_id").orderBy("premium", "id")
w_premium_tercile_by_premium_r = Window.partitionBy("premium_tercile").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_tercile_by_premium = Window.partitionBy("premium_tercile").orderBy("premium", "id")
w_premium_tercile_by_premium_f = Window.partitionBy("premium_tercile").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_policys = (
    df_policies
    .filter((F.col("premium") >= 1541.8) & (F.col("start_date") >= F.lit("2024-08-27").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

combined_details_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

ranked = (
    combined_details_holder
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "holder_name",
        "holder_joined_on",
        F.round(F.col("premium") - F.avg("premium").over(w_status_holder_joined_on_mont), 2).alias("premium_vs_status_holder_joined_on_month_avg"),
        F.round(F.avg("premium").over(w_holder_joined_on_month_holde), 2).alias("premium_avg_1_prior"),
        F.min("premium").over(w_status_holder_joined_on_mont).alias("status_holder_joined_on_month_min_premium"),
        F.lead("holder_joined_on").over(w_all_by_premium).alias("next_holder_joined_on"),
        F.lead("holder_joined_on", 3).over(w_all_by_premium).alias("next3_holder_joined_on"),
        F.round(F.avg("premium").over(w_status_holder_joined_on_mont), 2).alias("status_holder_joined_on_month_avg_premium"),
        F.round(F.cume_dist().over(w_holder_joined_on_month_holde_2), 3).alias("premium_cume_dist"),
        F.row_number().over(w_holder_joined_on_month_holde_3).alias("seq_no"),
        F.ntile(3).over(w_all_by_premium).alias("premium_tercile"),
        F.count(F.lit(1)).over(w_status_holder_joined_on_mont).alias("rows_in_status_holder_joined_on_month"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "holder_joined_on",
        "premium_vs_status_holder_joined_on_month_avg",
        "premium_avg_1_prior",
        "status_holder_joined_on_month_min_premium",
        "next3_holder_joined_on",
        "status_holder_joined_on_month_avg_premium",
        "premium_tercile",
        "rows_in_status_holder_joined_on_month",
        F.max("premium_cume_dist").over(w_premium_tercile_by_premium_r).alias("max_premium_cume_dist_so_far"),
        F.ntile(4).over(w_premium_tercile_by_premium).alias("premium_quartile"),
        F.nth_value("premium_cume_dist", 2).over(w_premium_tercile_by_premium_f).alias("second_premium_cume_dist"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "holder_id",
        "premium",
        "premium_avg_1_prior",
        "status_holder_joined_on_month_min_premium",
        "status_holder_joined_on_month_avg_premium",
        "premium_tercile",
        "rows_in_status_holder_joined_on_month",
        "max_premium_cume_dist_so_far",
        "premium_quartile",
        "second_premium_cume_dist",
        F.greatest(F.col("second_premium_cume_dist"), F.col("premium_avg_1_prior")).alias("greatest_second_premium_cume_dist_premium_avg_1_"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
    .orderBy(
        F.col("status_holder_joined_on_month_avg_premium").desc(),
        F.col("premium").desc(),
    )
)
