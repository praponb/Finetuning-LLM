from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last())
w4 = Window.partitionBy("city").orderBy(F.col("rating_rank").desc_nulls_first(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("product_brand").orderBy(F.col("rating_rank").desc_nulls_last(), "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
    )
)

enriched = (
    filtered
    .filter(F.col("rating") < 2.0)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("rating", ignorenulls=True).over(w2).alias("last_known_rating"),
        F.rank().over(w3).alias("rating_rank"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.supplier_name_filled",
        "s.row_seq",
        "s.last_known_rating",
        "s.rating_rank",
        F.col("c.brand").alias("product_brand"),
        F.col("c.stock_qty"),
    )
)

latest = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("product_brand")
    .agg(
        F.sort_array(F.collect_set("product_brand")).alias("product_brand_list"),
        F.sum("last_known_rating").alias("total_last_known_rating"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
    )
    .filter(F.col("stock_qty_p90") > 0)
)
