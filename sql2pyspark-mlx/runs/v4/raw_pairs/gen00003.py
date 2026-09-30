from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("rating").desc_nulls_first(), "id")
w3 = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_last())
w4 = Window.partitionBy("category").orderBy(F.col("cost").asc_nulls_first())
w5 = Window.partitionBy("category").orderBy(F.col("cost").desc())
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("category", "city")

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.0, "Medium").otherwise("Low").alias("rating_band"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.cost"))
)

deduped = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

matched_keys = (
    deduped
    .select("city")
    .subtract(
        df_suppliers
        .filter(~F.col("supplier_name").like("S%"))
        .select("city")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("category").isNotNull() & (F.col("rating") >= 2.8))
    .select(
        "id",
        "supplier_name",
        "city",
        "city_clean",
        "category",
        "cost",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
    )
)

scored = (
    base
    .select(
        "*",
        F.rank().over(w3).alias("cost_rank"),
        F.rank().over(w4).alias("cost_rank_2"),
        F.dense_rank().over(w5).alias("cost_dense_rank"),
        F.sum("cost").over(w6).alias("city_sum_cost"),
        F.max("cost").over(w7).alias("category_max_cost"),
    )
)

result = (
    scored
    .cube(
        "city",
        "category",
        F.when(F.col("cost_rank_2") >= 9, "LARGE").otherwise("SMALL").alias("cost_rank_2_tier"),
    )
    .agg(
        F.avg("cost_rank").alias("avg_cost_rank"),
        F.sum(F.when(F.col("category") == "Books", F.col("category_max_cost"))).alias("books_total_category_max_cost"),
        F.round(F.sum("cost_dense_rank") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("cost_dense_rank_per_london"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("books_total_category_max_cost") >= 0)
)
