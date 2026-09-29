from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("city_length").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.date_trunc("WEEK", "product_launch_date"))
w4 = Window.orderBy("city_length", "id")
w_part = Window.orderBy("rating_share", "id")
w6 = Window.orderBy("rating", "id")
w_recent = Window.orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_city", F.concat("country", F.lit("/"), "city"))
    .withColumn("city_length", F.length("city"))
    .withColumn("city_filled", F.coalesce("city", F.lit("OTHER")))
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "rating", "city_length", "city_filled")
)

with_latest = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

with_windows = (
    with_latest
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
        F.lead("rating", 2).over(w4).alias("next_rating"),
    )
)

enriched = (
    with_windows
    .filter(F.col("rating") <= 3.9)
    .select(
        "id",
        "rating_share",
        "next_rating",
        F.lead("next_rating", 1).over(w_part).alias("next_next_rating"),
        F.lag("rating_share", 1).over(w6).alias("prev_rating_share"),
        F.avg("rating").over(w_recent).alias("rating_avg_6"),
    )
)

with_latest_suppliers = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

result = (
    with_latest_suppliers
    .groupBy(
        F.when(F.col("prev_rating_share") >= 0.9, "PREMIUM").otherwise("STANDARD").alias("prev_rating_share_tier"),
        F.when(F.col("product_stock_qty") >= 362, "PREMIUM").otherwise("STANDARD").alias("product_stock_qty_tier"),
    )
    .agg(
        F.round(F.avg("product_stock_qty"), 2).alias("avg_product_stock_qty"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
)
