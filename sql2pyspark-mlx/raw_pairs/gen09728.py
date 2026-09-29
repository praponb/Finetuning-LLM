from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-04-23").cast("date")) & F.col("revenue").between(1100, 2500))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.add_months("sale_date", 3).alias("sale_date_plus_months"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .filter(F.size(F.col("b.tags")) >= 1)
    .select("a.id", "a.store_id", "a.product_id", F.col("b.category"))
)

valid = (
    with_dims
    .filter(F.col("store_id").isin([1, 5]))
    .withColumn("category_upper", F.upper("category"))
    .withColumn("category_filled", F.coalesce("category", F.lit("OTHER")))
)

valid_store_sales = (
    valid
    .filter(~F.col("category").eqNullSafe(F.lit("Sports")))
    .withColumn("category_title", F.initcap(F.trim("category")))
    .withColumn("is_books", F.when(F.col("category") == "Books", 1).otherwise(0))
)

result = (
    valid_store_sales
    .withColumn("is_books_tier", F.when(F.col("is_books") >= 83, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["is_books_tier", "product_id"], ["is_books_tier"], []],
        "is_books_tier",
        "product_id",
    )
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("is_books", 0.9).alias("is_books_p90"),
        F.grouping("is_books_tier").alias("grouping_id"),
    )
)
