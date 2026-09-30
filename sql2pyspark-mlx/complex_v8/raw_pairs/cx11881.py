from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("category", "unit_cost_tier").orderBy(F.col("unit_cost").desc(), F.col("id").desc())

valid = (
    df_skus
    .filter((~F.col("is_active")) | (F.col("weight_kg") > 6.5))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("unit_cost") >= 277.0, "A").when(F.col("unit_cost") >= 108.5, "B").otherwise("C").alias("unit_cost_tier"),
        F.array_contains(F.col("labels"), "bulky").alias("has_bulky"),
    )
)

unique_rows = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

scored = (
    unique_rows
    .filter(F.col("is_active"))
    .select(
        "*",
        F.when(F.col("unit_cost") >= 239.3, "large").when(F.col("unit_cost") >= 158.7, "medium").otherwise("small").alias("unit_cost_tier_2"),
        F.length(F.trim(F.col("unit_cost_tier"))).alias("unit_cost_tier_len"),
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
    )
)

filtered = (
    scored
    .crossJoin(scored.agg(F.max("unit_cost").alias("overall_max_unit_cost")))
    .filter(F.col("unit_cost") < F.col("overall_max_unit_cost"))
    .drop("overall_max_unit_cost")
)

result = (
    filtered
    .groupingSets(
        [["category", "unit_cost_tier_2"], ["category"], []],
        "category", "unit_cost_tier_2",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("category").alias("distinct_categorys"),
        F.sum("weight_kg").alias("total_weight_kg"),
    )
    .orderBy(
        F.col("category").asc_nulls_last(),
        F.col("unit_cost_tier_2").asc_nulls_last(),
    )
)
