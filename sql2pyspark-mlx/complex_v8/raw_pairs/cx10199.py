from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_unit_cost_tier = Window.partitionBy("unit_cost_tier")

base = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("A%"))
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
        F.when(F.col("unit_cost") >= 239.3, "large").when(F.col("unit_cost") >= 108.5, "medium").otherwise("small").alias("unit_cost_tier"),
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)

tagged = (
    base
    .select(
        "*",
        F.abs(F.col("weight_kg") - 15.2).alias("weight_kg_distance"),
        F.abs(F.col("weight_kg") - 14.1).alias("weight_kg_distance_2"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.round(F.col("is_active_flag") / F.nullif(F.sum("is_active_flag").over(w_unit_cost_tier), F.lit(0)), 4).alias("is_active_flag_share_of_unit_cost_tier"),
        F.round(F.col("is_active_flag") - F.avg("is_active_flag").over(w_unit_cost_tier), 2).alias("is_active_flag_vs_unit_cost_tier_avg"),
        F.max("weight_kg_distance").over(w_unit_cost_tier).alias("unit_cost_tier_max_weight_kg_distance"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.when(F.col("unit_cost_tier").isin("small", "medium"), "group_a").when(F.col("unit_cost_tier") == "large", "group_b").otherwise("other").alias("unit_cost_tier_group"),
        F.coalesce(F.col("unit_cost_tier"), F.lit("UNKNOWN")).alias("unit_cost_tier_or_unknown"),
        (F.floor(F.col("weight_kg") / 5) * 5).alias("weight_kg_bucket"),
    )
)

result = (
    annotated
    .select("id", "is_active_flag", "weight_kg_bucket")
    .unpivot(
        ids=["id"],
        values=["is_active_flag", "weight_kg_bucket"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
