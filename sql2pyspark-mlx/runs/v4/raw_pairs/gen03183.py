from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("category", "brand")
    .subtract(
        df_products
        .filter(F.col("category") == "Sports")
        .select("category", "brand")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
    )
    .orderBy(F.col("cost").desc_nulls_first())
)
