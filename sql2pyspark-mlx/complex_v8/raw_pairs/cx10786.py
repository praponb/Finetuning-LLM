from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_segment_by_policy = Window.partitionBy("policy_id", "segment").orderBy(F.col("policy_premium").desc(), "id", "policy_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_segment_by_policy_2 = Window.partitionBy("policy_id", "segment").orderBy(F.col("policy_premium").desc(), "id", "policy_id")
w_policy_id_segment_by_policy_3 = Window.partitionBy("policy_id", "segment").orderBy(F.col("policy_start_date").desc_nulls_first(), "id", "policy_id")

valid = (
    df_policyholders
    .filter((F.col("region") != "South") & (F.lower(F.col("holder_name")).like("%nt%")))
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
    )
)

line_level = (
    valid.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.segment",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

sequenced = (
    line_level
    .select(
        "*",
        F.last("policy_premium", ignorenulls=True).over(w_policy_id_segment_by_policy).alias("last_known_policy_premium"),
        F.dense_rank().over(w_policy_id_segment_by_policy_2).alias("policy_premium_dense_rank"),
        F.ntile(10).over(w_policy_id_segment_by_policy_2).alias("policy_premium_decile"),
        F.lag("policy_start_date").over(w_policy_id_segment_by_policy_3).alias("prev_policy_start_date"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["policy_id", "policy_premium_decile", "policy_premium_dense_rank"], ["policy_id"], []],
        "policy_id", "policy_premium_decile", "policy_premium_dense_rank",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("policy_premium_decile"), 2).alias("stddev_policy_premium_decile"),
        F.round(F.sum("policy_premium_decile") / F.nullif(F.sum("policy_premium"), F.lit(0)), 3).alias("policy_premium_decile_to_policy_premium_ratio"),
        F.array_sort(F.collect_set("segment")).alias("segment_list"),
    )
)
