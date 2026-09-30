from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_holder_joined_on = Window.orderBy("holder_joined_on", "id")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_premium_tier_status = Window.partitionBy("premium_tier", "status")
w_all_by_holder_joined_on_full = Window.orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status = Window.partitionBy("status")
w_holder_id_premium_tier_by_ho = Window.partitionBy("holder_id", "premium_tier").orderBy("holder_joined_on", "id").rowsBetween(-3, Window.currentRow)

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3817.5, "large").when(F.col("premium") >= 2111.3, "medium").otherwise("small").alias("premium_tier"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"), "p.holder_name")
)

metrics = (
    with_parent
    .select(
        "*",
        F.lead("end_date").over(w_all_by_holder_joined_on).alias("next_end_date"),
        F.row_number().over(w_all_by_end_date).alias("rn"),
        F.count(F.lit(1)).over(w_premium_tier_status).alias("rows_in_premium_tier_status"),
        F.nth_value("start_date", 2).over(w_all_by_holder_joined_on_full).alias("second_start_date"),
        F.dense_rank().over(w_all_by_holder_joined_on).alias("holder_joined_on_dense_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_premium_tier_status), F.lit(0)), 4).alias("premium_share_of_premium_tier_status"),
        F.min("premium").over(w_status).alias("status_min_premium"),
        F.round(F.avg("premium").over(w_holder_id_premium_tier_by_ho), 2).alias("premium_avg_4"),
        F.round(F.col("premium") - F.avg("premium").over(w_premium_tier_status), 2).alias("premium_vs_premium_tier_status_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "premium_tier",
        "holder_name",
        "next_end_date",
        "rows_in_premium_tier_status",
        "premium_avg_4",
    )
    .orderBy("holder_name", F.col("premium_tier").desc_nulls_first())
)
