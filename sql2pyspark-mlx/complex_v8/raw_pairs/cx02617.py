from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category").orderBy("unit_cost", "id")
w_dedup = Window.partitionBy("n_rows_tier").orderBy(F.col("n_rows").desc(), F.col("is_active").desc())

cleaned_skus = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        "is_active",
    )
)

unique_rows = (
    cleaned_skus
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

grouped = (
    unique_rows
    .groupBy("is_active")
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_active"), 1)) / F.count("*"), 1).alias("pct_is_active_false"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
    )
)

enriched = (
    grouped
    .filter(F.col("pct_is_active_false") > 35.0)
    .select(
        "is_active",
        "pct_is_active_false",
        "n_rows",
        "distinct_ids",
        "category_list",
        F.when(F.col("n_rows") >= 7, "large").when(F.col("n_rows") >= 4, "medium").otherwise("small").alias("n_rows_tier"),
    )
)

deduped = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupBy("n_rows", "is_active")
    .pivot("n_rows_tier", ["large", "small", "medium"])
    .agg(F.max("pct_is_active_false"))
    .orderBy("n_rows", "is_active")
)
