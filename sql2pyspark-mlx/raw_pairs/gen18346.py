from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date_minus_19d", "id")
w3 = Window.partitionBy("product_id").orderBy("sqft", "id")

base = (
    df_store_sales
    .filter(F.col("revenue") > 1300)
    .select("*", F.date_sub("sale_date", 19).alias("sale_date_minus_19d"))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_store_sales = (
    ranked
    .withColumn("dedup_rank_2", F.row_number().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
    .select("id", "store_id", "product_id")
)

with_dims = (
    ranked_store_sales
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.product_name").like("Yog%"))
    .select("s.*", F.col("d.product_name"))
)

valid = (
    with_dims
    .filter(F.col("product_name").like("%Canc%"))
    .withColumn("product_name_upper", F.upper("product_name"))
)

joined = (
    valid
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.sqft"), F.col("d.region"), F.col("d.opened_date"))
)

with_windows = (
    joined
    .select(
        "id",
        "store_id",
        "product_id",
        "product_name_upper",
        "sqft",
        "opened_date",
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(10).over(w3).alias("sqft_bucket"),
    )
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("sqft_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("sqft_bucket_tier"),
        "product_id",
        "store_id",
    )
    .agg(
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50_2"),
    )
)

result = (
    daily
    .select(
        "sqft_bucket_tier",
        "product_id",
        "store_id",
        "sqft_p50",
        "num_rows",
        "sqft_p50_2",
    )
    .orderBy(F.col("product_id").desc(), F.col("sqft_p50").desc())
)
