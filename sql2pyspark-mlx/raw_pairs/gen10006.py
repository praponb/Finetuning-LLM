from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("running_count_rating").desc_nulls_last(), "id")

metrics = (
    df_suppliers
    .select(
        "*",
        F.min("rating").over(w).alias("running_min_rating"),
        F.sum("rating").over(w).alias("running_sum_rating"),
        F.max("rating").over(w).alias("running_max_rating"),
        F.count("rating").over(w).alias("running_count_rating"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.year(F.col("c.launch_date")) == 2023), "left_anti")
)

latest = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "country",
        "city",
        "running_min_rating",
        "running_max_rating",
        "running_count_rating",
    )
)

remaining_keys = (
    latest
    .select("id")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") <= 2.0)
        .select("id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible_suppliers = (
    verified
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= "2025-05-01"), "left_semi")
)

filtered = (
    eligible_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Tokyo"))) & (F.col("running_min_rating") > 3.5))
    .select(
        "id",
        "city",
        "running_max_rating",
        "running_count_rating",
        F.when(F.col("running_count_rating") >= 14, "High").when(F.col("running_count_rating") >= 4, "Medium").otherwise("Low").alias("running_count_rating_tier"),
    )
)

result = (
    filtered
    .select(
        "city",
        "running_max_rating",
        "running_count_rating",
        "running_count_rating_tier",
    )
    .orderBy(F.col("running_max_rating").asc_nulls_first(), "running_count_rating")
)
