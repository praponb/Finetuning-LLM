from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_product = Window.partitionBy(F.year("start_date"), "product")
w_holder_id_holder_joined_on_m = Window.partitionBy("holder_id", F.month("holder_joined_on"))
w_all_by_holder_joined_on_prio = Window.orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-3, -1)
w_all_by_holder_joined_on_ties = Window.orderBy(F.col("holder_joined_on").desc())
w_all_by_start_date = Window.orderBy("start_date", "id")
w_holder_joined_on_month_by_st = Window.partitionBy(F.month("holder_joined_on")).orderBy("start_date")
w_holder_joined_on_month_by_st_2 = Window.partitionBy(F.month("holder_joined_on")).orderBy("start_date", "id")
w_product_by_premium_ties = Window.partitionBy("product").orderBy("premium")
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_start_date_year = Window.partitionBy(F.year("start_date"))

filtered_policys = (
    df_policies
    .filter((F.col("product") == "LIFE") & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

combined_details = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "holder_joined_on",
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_year_product), 2).alias("premium_vs_start_date_year_product_avg"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_holder_joined_on_m), 2).alias("premium_vs_holder_id_holder_joined_on_month_avg"),
        F.sum("premium").over(w_all_by_holder_joined_on_prio).alias("premium_sum_3_prior"),
        F.dense_rank().over(w_all_by_holder_joined_on_ties).alias("holder_joined_on_dense_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_year_product), F.lit(0)), 4).alias("premium_share_of_start_date_year_product"),
        F.lead("start_date").over(w_all_by_start_date).alias("next_start_date"),
        F.round(F.cume_dist().over(w_holder_joined_on_month_by_st), 3).alias("start_date_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_holder_joined_on_ties), 3).alias("holder_joined_on_percent_rank"),
        (F.col("premium") - F.lag("premium").over(w_holder_joined_on_month_by_st_2)).alias("premium_change"),
        F.rank().over(w_product_by_premium_ties).alias("premium_rank"),
        F.lead("start_date", 2).over(w_product_by_premium).alias("next2_start_date"),
        F.max("premium").over(w_start_date_year).alias("start_date_year_max_premium"),
        F.round(F.cume_dist().over(w_product_by_premium_ties), 3).alias("premium_cume_dist"),
        F.size(F.collect_set("product").over(w_holder_id_holder_joined_on_m)).alias("distinct_product_in_holder_id_holder_joined_on_m"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("premium_rank") <= 5)
    .drop("premium_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "holder_id",
        "product",
        "holder_joined_on",
        "premium_vs_start_date_year_product_avg",
        "premium_vs_holder_id_holder_joined_on_month_avg",
        "premium_share_of_start_date_year_product",
        "next_start_date",
        "start_date_cume_dist",
        "holder_joined_on_percent_rank",
        "premium_change",
        "next2_start_date",
        "start_date_year_max_premium",
        "premium_cume_dist",
        "distinct_product_in_holder_id_holder_joined_on_m",
    )
    .orderBy(
        "premium_vs_holder_id_holder_joined_on_month_avg",
        "holder_joined_on_percent_rank",
    )
)
