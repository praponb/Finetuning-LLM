from pyspark.sql import functions as F

revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

matched_keys = (
    df_store_sales
    .select("store_id", "product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("revenue") < 2900)
        .select("store_id", "product_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("product_id").desc_nulls_first())
)
