from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc())
w_part = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country").orderBy("running_min_rating", "id").rowsBetween(-6, -1)

ranked = (
    df_suppliers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "supplier_name", "country", "city", "rating")
)

kept = (
    ranked
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.size(F.col("ch.tags")) > 2), "left_semi")
)

enriched = (
    kept
    .select(
        "*",
        F.max("rating").over(w_part).alias("running_max_rating"),
        F.sum("rating").over(w_part).alias("running_sum_rating"),
        F.min("rating").over(w_part).alias("running_min_rating"),
    )
)

with_windows = (
    enriched
    .filter(F.col("running_min_rating").between(2.6, 2.9))
    .select(
        "*",
        F.max("rating").over(w3).alias("city_max_rating"),
        F.avg("running_min_rating").over(w4).alias("running_min_rating_avg_6_prior"),
    )
)

kept_suppliers = (
    with_windows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") >= 690), "left_anti")
)

by_type = (
    kept_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "DE"])
    .agg(F.sum("city_max_rating").alias("amount"))
)

result = (
    by_type
    .select("city", "SG", "DE")
    .orderBy(F.col("city").desc_nulls_last())
)
