from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("supplier_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("supplier_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_city").orderBy(F.col("rating").desc())
w5 = Window.partitionBy("running_supplier_rating_tier").orderBy(F.col("initech_total_last_known_supplier_rating").desc())

joined = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_name").isNotNull())
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.tags",
        "src.supplier_id",
        F.col("p.rating"),
        F.col("p.city"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.supplier_name"),
        F.col("d.rating").alias("supplier_rating"),
        F.col("d.city").alias("supplier_city"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.last("supplier_rating", ignorenulls=True).over(w).alias("last_known_supplier_rating"),
        F.sum("supplier_rating").over(w2).alias("running_supplier_rating"),
        F.last("rating", ignorenulls=True).over(w3).alias("last_known_rating"),
    )
)

base = (
    with_windows
    .filter(~F.col("brand").eqNullSafe(F.lit("Umbrella")))
    .select(
        "id",
        "brand",
        "tags",
        "supplier_id",
        "rating",
        "supplier_city",
        "last_known_supplier_rating",
        "running_supplier_rating",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
        F.trim("brand").alias("clean_brand"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.when(F.col("running_supplier_rating") >= 20, "LARGE").otherwise("SMALL").alias("running_supplier_rating_tier"),
        "supplier_id",
    )
    .agg(
        F.max(F.when(F.col("brand") == "Globex", F.col("last_known_supplier_rating"))).alias("globex_max_last_known_supplier_rating"),
        F.round(F.sum("running_supplier_rating") / F.nullif(F.count(F.when(F.col("supplier_city") == "Singapore", 1)), F.lit(0)), 2).alias("running_supplier_rating_per_singapore"),
        F.count(F.when(F.col("supplier_city") == "Bangkok", 1)).alias("bangkok_count"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("last_known_supplier_rating"))).alias("initech_total_last_known_supplier_rating"),
    )
    .filter(F.col("bangkok_count") > 1)
    .withColumn("initech_total_last_known_supplier_rating_percent_rank", F.percent_rank().over(w5))
    .orderBy(F.col("bangkok_count").desc())
)
