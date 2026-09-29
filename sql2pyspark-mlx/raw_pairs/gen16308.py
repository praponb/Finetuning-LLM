from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last(), "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("country_avg_rating")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, -1)
w8 = Window.orderBy("rating_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.year("product_launch_date"), "city").orderBy("country_avg_rating", "id").rowsBetween(-2, -1)
w10 = Window.partitionBy("country", "product_category").orderBy(F.col("country_avg_rating").desc())
w11 = Window.partitionBy(F.date_trunc("week", "product_launch_date"), "product_category").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("country_avg_rating_avg_2_prior", "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.lower("supplier_name").alias("supplier_name_lower"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .filter(F.col("country") == "SG")
    .select(
        "id",
        "country",
        "city",
        "rating",
        "row_num",
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.avg("rating").over(w2).alias("country_avg_rating"),
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.category").alias("product_category"),
        F.col("c.price").alias("product_price"),
    )
)

scored = (
    with_latest
    .select(
        "id",
        "city",
        "rating",
        "rating_share",
        "country_avg_rating",
        "dedup_rank",
        "product_launch_date",
        "product_price",
        F.avg("country_avg_rating").over(w7).alias("country_avg_rating_avg_2_prior"),
        F.last("product_category", ignorenulls=True).over(w8).alias("last_known_product_category"),
        F.avg("rating_share").over(w9).alias("rating_share_avg_2_prior"),
        F.dense_rank().over(w10).alias("country_avg_rating_dense_rank"),
        F.sum("country_avg_rating").over(w11).alias("cumulative_country_avg_rating"),
    )
)

result = (
    scored
    .select(
        "id",
        "rating",
        "product_launch_date",
        "product_price",
        "country_avg_rating_avg_2_prior",
        "rating_share_avg_2_prior",
        "country_avg_rating_dense_rank",
        "cumulative_country_avg_rating",
        F.lag("rating", 1, 0).over(w_recent).alias("prev_rating"),
        F.ntile(3).over(w_recent).alias("country_avg_rating_avg_2_prior_bucket"),
    )
)
