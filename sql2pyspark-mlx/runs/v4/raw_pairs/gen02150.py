from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rating_share", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("city").orderBy("launch_date", "id")
w7 = Window.partitionBy("city").orderBy(F.col("rating_share").desc())
w8 = Window.partitionBy("city").orderBy(F.col("city_count_rating_share").desc(), "id")
w9 = Window.partitionBy("city").orderBy("rating_share", "id").rowsBetween(-2, -1)
w10 = Window.orderBy("first_known_rating_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("city").orderBy("rating_share_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("city").orderBy("rating_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("city").orderBy("city_count_rating_share", "id")

base = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "lion city imports")
    .select(
        "id",
        "city",
        "rating",
        F.upper("city").alias("city_upper"),
        F.concat_ws("/", "city", "country").alias("city_country_key"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

base_suppliers = (
    base
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

not_flagged = (
    base_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.category") == "Sports"), "left_anti")
)

scored = (
    not_flagged
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("prev_rating", F.lag("rating", 1).over(w2))
    .withColumn("city_count_rating", F.count("rating").over(w))
)

metrics = (
    scored
    .withColumn("first_known_rating_share", F.first("rating_share", ignorenulls=True).over(w3))
    .withColumn("city_count_rating_share", F.round((F.col("city_count_rating") / F.sum("city_count_rating").over(w)), 4))
    .withColumn("rating_avg_5", F.avg("rating").over(w4))
)

with_last = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

with_windows = (
    with_last
    .select(
        "*",
        F.first("rating", ignorenulls=True).over(w3).alias("first_known_rating"),
        F.lag("prev_rating", 1).over(w6).alias("prev_prev_rating"),
        F.rank().over(w7).alias("rating_share_rank"),
        F.round((F.col("rating_avg_5") / F.sum("rating_avg_5").over(w)), 4).alias("rating_avg_5_share"),
        F.ntile(3).over(w8).alias("city_count_rating_share_bucket"),
    )
)

result = (
    with_windows
    .withColumn("rating_avg_5_share_avg_2_prior", F.avg("rating_avg_5_share").over(w9))
    .withColumn("last_known_prev_rating", F.last("prev_rating", ignorenulls=True).over(w10))
    .withColumn("first_known_city_count_rating_share", F.first("city_count_rating_share", ignorenulls=True).over(w11))
    .withColumn("running_rating_avg_5_share", F.sum("rating_avg_5_share").over(w12))
    .withColumn("next_prev_prev_rating", F.lead("prev_prev_rating", 1).over(w13))
)
