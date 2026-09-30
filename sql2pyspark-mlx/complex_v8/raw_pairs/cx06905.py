from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_all_by_premium_filled_full = Window.orderBy(F.col("premium_filled").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product = Window.partitionBy("product")
w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy("premium", "id").rowsBetween(-6, Window.currentRow)
w_holder_region_holder_id_by_p = Window.partitionBy("holder_region", "holder_id").orderBy("premium", "id").rowsBetween(-2, Window.currentRow)
w_holder_id_status_by_premium_2 = Window.partitionBy("holder_id", "status").orderBy("premium", "id")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy(F.col("premium_filled").desc(), "id")
w_holder_id_by_premium_filled = Window.partitionBy("holder_id").orderBy(F.col("premium_filled").desc())
w_all_by_premium_filled_moving = Window.orderBy(F.col("premium_filled").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_product_by_premium_ties = Window.partitionBy("product").orderBy("premium")
w_status_by_premium_ties = Window.partitionBy("status").orderBy("premium")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy("premium_max_3", "id")
w_premium_dense_rank = Window.partitionBy("premium_dense_rank")
w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy("premium_max_3", "id").rowsBetween(-1, Window.currentRow)

prepared_policys = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "LAPSED", "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

joined_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.status_slug",
        "s.premium_filled",
        F.col("p.region").alias("holder_region"),
    )
)

sequenced = (
    joined_holder
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.nth_value("premium_filled", 2).over(w_all_by_premium_filled_full).alias("second_premium_filled"),
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.round(F.avg("premium").over(w_holder_id_status_by_premium), 2).alias("premium_avg_7"),
        F.max("premium").over(w_holder_region_holder_id_by_p).alias("premium_max_3"),
        F.lag("premium").over(w_holder_id_status_by_premium_2).alias("prev_premium"),
        F.lag("premium").over(w_product_holder_id_by_premium).alias("prev_premium_2"),
        F.round(F.cume_dist().over(w_holder_id_by_premium_filled), 3).alias("premium_filled_cume_dist"),
        F.round(F.avg("premium").over(w_all_by_premium_filled_moving), 2).alias("premium_avg_2"),
        F.dense_rank().over(w_product_by_premium_ties).alias("premium_dense_rank"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("premium_dense_rank") <= 5) & (F.col("premium_filled_cume_dist") >= 0.1))
)

ranked = (
    first_rows
    .select(
        "*",
        F.dense_rank().over(w_status_by_premium_ties).alias("premium_dense_rank_2"),
        F.lag("premium_avg_7", 3).over(w_status_holder_id_by_premium).alias("prev3_premium_avg_7"),
        F.lead("premium_vs_holder_id_avg", 2).over(w_status_holder_id_by_premium).alias("next2_premium_vs_holder_id_avg"),
        F.min("premium_avg_7").over(w_premium_dense_rank).alias("premium_dense_rank_min_premium_avg_7"),
        F.row_number().over(w_status_by_premium).alias("rn"),
        F.min("premium_vs_holder_id_avg").over(w_premium_dense_rank).alias("premium_dense_rank_min_premium_vs_holder_id_avg"),
        F.round(F.avg("premium_dense_rank").over(w_status_holder_id_by_premium_2), 2).alias("premium_dense_rank_avg_2"),
    )
)

result = (
    ranked
    .filter(F.col("premium_vs_holder_id_avg") < 2000.0)
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "status_slug",
        "premium_filled",
        "holder_region",
        "premium_vs_holder_id_avg",
        "product_max_premium",
        "premium_avg_7",
        "premium_max_3",
        "prev_premium",
        "prev_premium_2",
        "premium_filled_cume_dist",
        "premium_avg_2",
        "premium_dense_rank",
        "premium_dense_rank_2",
        "prev3_premium_avg_7",
        "premium_dense_rank_min_premium_avg_7",
        "rn",
        "premium_dense_rank_min_premium_vs_holder_id_avg",
        "premium_dense_rank_avg_2",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.when(F.col("holder_region").isin("West", "South"), "group_a").when(F.col("holder_region") == "Central", "group_b").otherwise("other").alias("holder_region_group"),
    )
    .orderBy(F.col("prev_premium").desc())
)
