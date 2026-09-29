from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

enriched = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.brand",
        "a.stock_qty",
        "a.launch_date",
        "a.tags",
        "a.supplier_id",
        F.col("s.supplier_name"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "category", "brand", "launch_date", "tags")
)

remaining_keys = (
    latest
    .select("brand", "category")
    .exceptAll(
        df_products
        .filter(F.length("product_name") < 13)
        .select("brand", "category")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    verified
    .select("brand")
    .subtract(
        df_products
        .filter(F.col("launch_date").between("2023-05-01", "2025-05-17"))
        .select("brand")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

remaining_keys_3 = (
    kept_rows
    .select("id", "brand")
    .subtract(
        df_products
        .filter(F.col("cost").between(270, 510))
        .select("id", "brand")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(remaining_keys_3.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("brand")
    .agg(
        F.count_if(F.col("category") == "Grocery").alias("grocery_events"),
        F.countDistinct("category").alias("distinct_categories"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
    )
    .orderBy(F.col("brand").desc_nulls_first())
)
