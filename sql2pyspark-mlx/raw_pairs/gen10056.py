from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("product_name_length").desc(), "id")
w2 = Window.partitionBy("week_start").orderBy(F.col("corr_cost_price").desc())

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        F.length("product_name").alias("product_name_length"),
        F.greatest("cost", "price").alias("greatest_cost_price"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter(F.col("launch_date") > "2024-04-01")
    .withColumn("launch_date_label", F.date_format("launch_date", "yyyy-MM"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.revenue") > 3600), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([2, 3, 4, 6])), "left_anti")
)

remaining_keys = (
    kept
    .select("id", "brand")
    .exceptAll(
        df_products
        .filter(F.col("price") >= 750)
        .select("id", "brand")
    )
)

verified = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.when(F.col("price") >= 1040, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.corr("cost", "price").alias("corr_cost_price"),
    )
    .withColumn("corr_cost_price_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("week_start").desc_nulls_first())
)
