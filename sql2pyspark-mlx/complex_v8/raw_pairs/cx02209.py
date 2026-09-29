from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category").orderBy("weight_kg", "id", "label")

base = (
    df_skus
    .filter(F.col("weight_kg") > 12.4)
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
    )
)

flattened = (
    base
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "is_active",
        "category_slug",
        F.explode("labels").alias("label"),
    )
)

category_stats = (
    flattened
    .groupBy("category")
    .agg(F.max("weight_kg").alias("max_weight_kg"))
)

above_average = (
    flattened.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg") == F.col("g.max_weight_kg"))
    .select(
        "s.*",
        "g.max_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.max_weight_kg"), 2).alias("weight_kg_minus_max"),
    )
)

latest_per_key = (
    above_average
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupingSets(
        [["is_active", "label", "category"], ["is_active"], ["label"]],
        "is_active", "label", "category",
    )
    .agg(F.count("*").alias("row_count"))
)
