from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "category").orderBy("sale_date_period", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-23").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        F.date_trunc("week", "sale_date").alias("sale_date_period"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.sale_date_period",
        F.col("d.category"),
        F.col("d.brand"),
    )
)

latest = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    latest
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.sale_date",
        "s.sale_date_period",
        "s.brand",
        "s.latest_rank",
        F.col("d.category").alias("product_category"),
    )
)

matched_keys = (
    with_dims
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue") <= 3000)
        .select("id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("WEEK", "sale_date_period").alias("week_start"))
    .agg(
        F.count(F.when(F.col("product_category") == "Home", 1)).alias("home_rows"),
        F.count(F.when(F.col("product_category") == "Books", 1)).alias("books_rows"),
        ((F.unix_timestamp(F.max("sale_date_period")) - F.unix_timestamp(F.min("sale_date_period"))) / 60).alias("duration_min"),
    )
    .orderBy("books_rows")
)
