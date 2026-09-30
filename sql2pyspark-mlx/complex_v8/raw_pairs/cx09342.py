from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_start_da = Window.partitionBy("holder_id", "status").orderBy("start_date", "id")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_holder_id_status_by_start_da_2 = Window.partitionBy("holder_id", "status").orderBy("start_date")
w_product_group_status_by_star = Window.partitionBy("product_group", "status").orderBy("start_date", "id")
w_product_group_status_by_star_2 = Window.partitionBy("product_group", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_policys = (
    df_policies
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("product").isin("LIFE", "TRAVEL"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
    )
)

windowed = (
    recent_policys
    .select(
        "id",
        "product",
        "start_date",
        "status",
        "product_group",
        F.lead("premium", 2).over(w_holder_id_status_by_start_da).alias("next2_premium"),
        (F.col("premium") - F.lag("premium").over(w_all_by_start_date)).alias("premium_change"),
        F.size(F.collect_set("status").over(w_product_holder_id)).alias("distinct_status_in_product_holder_id"),
        F.round(F.cume_dist().over(w_holder_id_status_by_start_da), 3).alias("start_date_cume_dist"),
        F.round(F.avg("premium").over(w_product_holder_id), 2).alias("product_holder_id_avg_premium"),
        F.lag("start_date").over(w_holder_id_status_by_start_da).alias("prev_start_date"),
        F.lag("premium").over(w_all_by_start_date).alias("prev_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_holder_id), F.lit(0)), 4).alias("premium_share_of_product_holder_id"),
        F.sum("premium").over(w_product_holder_id).alias("product_holder_id_sum_premium"),
        F.round(F.percent_rank().over(w_holder_id_status_by_start_da_2), 3).alias("start_date_percent_rank"),
        F.lag("start_date").over(w_product_group_status_by_star).alias("prev_start_date_2"),
        F.first("start_date", ignorenulls=True).over(w_product_group_status_by_star_2).alias("first_known_start_date"),
    )
)

tagged = (
    windowed
    .select(
        "id",
        "product",
        "start_date",
        "product_group",
        "premium_change",
        "distinct_status_in_product_holder_id",
        "start_date_cume_dist",
        "product_holder_id_avg_premium",
        "prev_premium",
        "premium_share_of_product_holder_id",
        "product_holder_id_sum_premium",
        "start_date_percent_rank",
        "prev_start_date_2",
        "first_known_start_date",
        F.concat(F.col("product_group"), F.lit("#"), F.col("id").cast("string")).alias("product_group_ref"),
        F.least(F.col("prev_premium"), F.col("premium_change")).alias("least_prev_premium_premium_change"),
        (F.col("premium_change") + F.col("start_date_percent_rank")).alias("premium_change_plus_start_date_percent_rank"),
    )
)

result = (
    tagged
    .select(
        "id",
        "product",
        "start_date",
        "product_group",
        "distinct_status_in_product_holder_id",
        "start_date_cume_dist",
        "prev_premium",
        "product_holder_id_sum_premium",
        "start_date_percent_rank",
        "prev_start_date_2",
        "product_group_ref",
        "least_prev_premium_premium_change",
        "premium_change_plus_start_date_percent_rank",
    )
    .orderBy(F.col("least_prev_premium_premium_change").desc_nulls_first())
)
