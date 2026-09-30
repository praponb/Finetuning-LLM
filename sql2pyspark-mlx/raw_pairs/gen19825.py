from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.orderBy("cost", "id")
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("product_brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_brand").orderBy("prev_cost", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("product_brand")
w8 = Window.partitionBy(F.date_trunc("MONTH", "launch_date"), "product_brand").orderBy("first_known_rating", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("product_brand").orderBy(F.col("first_known_rating").desc_nulls_last())
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w11 = Window.partitionBy("category").orderBy("product_brand_min_first_known_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("category").orderBy("product_launch_date", "id")
w13 = Window.partitionBy(F.date_trunc("YEAR", "product_launch_date"), "category").orderBy(F.col("first_known_rating_dense_rank").desc_nulls_last())
w14 = Window.orderBy(F.col("first_known_rating").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("New York"))))
    .select(
        "id",
        "supplier_name",
        "rating",
        F.substring("country", 1, 4).alias("country_prefix"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

with_windows = (
    latest_match
    .filter(F.col("rating") > 4.0)
    .select(
        "id",
        "supplier_name",
        F.lag("cost", 2).over(w2).alias("prev_cost"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
        F.lead("cost", 1).over(w2).alias("next_cost"),
        F.first("rating", ignorenulls=True).over(w3).alias("first_known_rating"),
    )
)

latest_match_suppliers = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.prev_cost",
        "s.first_known_rating",
        F.col("c.launch_date"),
        F.col("c.brand").alias("product_brand"),
    )
)

scored = (
    latest_match_suppliers
    .select(
        "id",
        "supplier_name",
        "prev_cost",
        "first_known_rating",
        F.sum("prev_cost").over(w5).alias("running_prev_cost"),
        F.avg("first_known_rating").over(w6).alias("first_known_rating_avg_2_prior"),
        F.min("first_known_rating").over(w7).alias("product_brand_min_first_known_rating"),
        F.avg("first_known_rating").over(w8).alias("first_known_rating_avg_2"),
        F.dense_rank().over(w9).alias("first_known_rating_dense_rank"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.first_known_rating",
        "s.product_brand_min_first_known_rating",
        "s.first_known_rating_dense_rank",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.category"),
    )
)

metrics = (
    with_last
    .withColumn("cumulative_first_known_rating", F.sum("first_known_rating").over(w11))
    .withColumn("event_seq", F.row_number().over(w12))
    .withColumn("first_known_rating_dense_rank_dense_rank", F.dense_rank().over(w13))
    .withColumn("row_seq", F.row_number().over(w14))
)

result = (
    metrics
    .select(
        "id",
        "product_brand_min_first_known_rating",
        "first_known_rating_dense_rank",
        "category",
        "cumulative_first_known_rating",
        "event_seq",
        "row_seq",
        F.when(F.col("category").rlike("^(Books|Sports)$"), "PRIORITY").otherwise("EXTENDED").alias("category_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
    .limit(20)
)
