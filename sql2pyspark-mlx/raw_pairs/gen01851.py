from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("product_launch_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("product_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy(F.col("last4_sum_product_price").desc(), "id")
w5 = Window.partitionBy("city", "country")
w6 = Window.orderBy(F.col("rating").asc_nulls_last())

base = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%ion%"))
    .select(
        "*",
        F.concat_ws("-", "country", "city").alias("country_city_key"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        "s.country_city_key",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.price").alias("product_price"),
    )
)

metrics = (
    latest_match
    .withColumn("last4_min_product_price", F.min("product_price").over(w2))
    .withColumn("last4_sum_product_price", F.sum("product_price").over(w2))
    .withColumn("last4_avg_product_price", F.avg("product_price").over(w2))
    .withColumn("last4_max_product_price", F.max("product_price").over(w2))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w3))
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    ranked
    .select("country", "id")
    .subtract(
        df_suppliers
        .filter(F.col("rating").between(2.8, 3.0))
        .select("country", "id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("city_sum_rating", F.sum("rating").over(w5))
    .withColumn("rating_rank", F.rank().over(w6))
)

result = (
    enriched
    .withColumn("last4_sum_product_price_tier", F.when(F.col("last4_sum_product_price") >= 4600, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "product_launch_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "product_launch_date"))
    .groupingSets(
        [["last4_sum_product_price_tier", "week_start", "year_start"], ["last4_sum_product_price_tier", "week_start"], []],
        "last4_sum_product_price_tier",
        "week_start",
        "year_start",
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.round(F.sum("last4_min_product_price") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("last4_min_product_price_per_singapore"),
        F.grouping("last4_sum_product_price_tier").alias("grouping_level"),
    )
    .orderBy(F.col("last4_sum_product_price_tier").asc_nulls_last())
)
