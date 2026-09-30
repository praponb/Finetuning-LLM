from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy(F.month("product_launch_date"), "city")
w4 = Window.partitionBy(F.date_trunc("week", "product_launch_date"))
w5 = Window.orderBy(F.col("rating_int").desc_nulls_last())

base = (
    df_suppliers
    .filter(F.col("rating") <= 3.6)
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.substring("supplier_name", 1, 4).alias("supplier_name_prefix"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") <= F.col("s.product_launch_date")), "left_semi")
)

kept_keys = (
    kept
    .select("id")
    .intersect(
        df_suppliers
        .filter(F.col("country") == "UK")
        .select("id")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("rating") < 3.6)
    .select(
        "*",
        F.when(F.col("city").isin(["London", "Singapore"]), "Priority").otherwise("Other").alias("city_group"),
        F.col("rating").cast("int").alias("rating_int"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_windows = (
    filtered
    .filter(F.col("product_launch_date").between("2024-05-13", "2025-09-08"))
    .select(
        "*",
        F.max("rating_int").over(w2).alias("city_max_rating_int"),
        F.stddev_samp("rating").over(w2).alias("city_stddev_samp_rating"),
        F.sum("rating").over(w3).alias("product_launch_date_month_sum_rating"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "rating",
        "rating_int",
        "resolved_city",
        "city_max_rating_int",
        "city_stddev_samp_rating",
        F.round((F.col("city_stddev_samp_rating") / F.sum("city_stddev_samp_rating").over(w4)), 4).alias("city_stddev_samp_rating_share"),
        F.percent_rank().over(w5).alias("rating_int_percent_rank"),
        F.sum("rating").over(w2).alias("city_sum_rating"),
    )
)

result = (
    enriched
    .select(
        "rating",
        "rating_int",
        "city_max_rating_int",
        "city_stddev_samp_rating",
        "city_stddev_samp_rating_share",
        "rating_int_percent_rank",
    )
)
