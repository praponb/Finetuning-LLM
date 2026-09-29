from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-12-06").cast("date"))
    .select("*", F.round((F.col("price") / F.col("cost")), 2).alias("margin"))
)

result = (
    base
    .cube(
        F.when(F.col("price") >= 980, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", F.col("price"))).alias("grocery_total_price"),
        F.count_if(F.col("category") == "Books").alias("books_events"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("price_tier").asc_nulls_last())
)
