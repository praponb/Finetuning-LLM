from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.product_name").isNull()), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.ntile(5).over(w).alias("rating_bucket"),
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
        F.sum("rating").over(w3).alias("running_rating"),
    )
)

kept_keys = (
    metrics
    .select("id", "city")
    .exceptAll(
        df_suppliers
        .filter(F.length("supplier_name") > 6)
        .select("id", "city")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") > 345), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("rating") >= 4.3, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.round(F.sum("rating_bucket") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("rating_bucket_per_de"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
    )
    .orderBy(F.col("chiang_mai_rows").desc_nulls_first())
)
