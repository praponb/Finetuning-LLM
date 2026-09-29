from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("brand", "city").orderBy(F.col("rating").asc_nulls_last())
w6 = Window.orderBy("rating_bucket", "id").rowsBetween(-3, -1)
w_part = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city").orderBy("rating_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_brand").orderBy("rating_avg_3_prior", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.ntile(4).over(w2).alias("rating_bucket"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

with_last_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"))
)

latest = (
    with_last_suppliers
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "city", "rating", "rating_bucket", "launch_date")
)

with_last_suppliers_2 = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"))
)

enriched = (
    with_last_suppliers_2
    .filter(~F.col("product_brand").isin(["Globex", "Umbrella", "Acme"]))
    .select(
        "id",
        "city",
        "rating",
        "rating_bucket",
        "launch_date",
        "product_brand",
        F.avg("rating").over(w6).alias("rating_avg_3_prior"),
        F.sum("rating").over(w_part).alias("cumulative_rating"),
    )
)

result = (
    enriched
    .select(
        "id",
        "rating_bucket",
        "rating_avg_3_prior",
        "cumulative_rating",
        F.min("cumulative_rating").over(w8).alias("running_min_cumulative_rating"),
        F.sum("cumulative_rating").over(w8).alias("running_sum_cumulative_rating"),
        F.avg("cumulative_rating").over(w8).alias("running_avg_cumulative_rating"),
        F.count("cumulative_rating").over(w8).alias("running_count_cumulative_rating"),
        F.avg("rating_bucket").over(w9).alias("rating_bucket_avg_5"),
    )
)
