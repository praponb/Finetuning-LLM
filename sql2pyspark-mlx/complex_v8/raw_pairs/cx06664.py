from pyspark.sql import functions as F

base_skus = (
    df_skus
    .filter(F.col("weight_kg") > 4.7)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
    )
)

sku_rows = (
    base_skus
    .filter(F.col("is_active"))
    .select(
        F.lit("sku").alias("origin"),
        F.col("id").alias("qty"),
        F.col("category").alias("label"),
    )
)

warehouse_side = (
    df_warehouses
    .filter(F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-02-14").cast("date")))
    .select(
        F.lit("warehouse").alias("origin"),
        F.col("id").alias("qty"),
        F.col("wh_code").alias("label"),
    )
)

sku_rows_2 = (
    df_skus
    .select(
        F.lit("sku_3").alias("origin"),
        F.col("id").alias("qty"),
        F.col("category").alias("label"),
    )
)

unioned = (
    sku_rows
    .union(warehouse_side)
    .union(sku_rows_2)
)

result = (
    unioned
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("origin")
)
