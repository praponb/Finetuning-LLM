from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("units").asc_nulls_first())
w2 = Window.partitionBy("city")
w_running = Window.partitionBy("store_id").orderBy(F.col("sale_date_day").asc_nulls_last())
w4 = Window.partitionBy(F.date_trunc("YEAR", "sale_date_plus_months"))
w5 = Window.orderBy(F.col("units").asc_nulls_first())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

remaining_keys = (
    df_store_sales
    .select("id", "store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id") == 8)
        .select("id", "store_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-06-24"))) & (F.col("revenue") < 2800))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.add_months("sale_date", 6).alias("sale_date_plus_months"),
        F.dayofmonth("sale_date").alias("sale_date_day"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.region"), F.col("p.store_name"), F.col("p.city"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.percent_rank().over(w).alias("units_percent_rank"),
        F.min("sale_date_day").over(w2).alias("city_min_sale_date_day"),
        F.rank().over(w_running).alias("sale_date_day_rank"),
        F.round((F.col("units") / F.sum("units").over(w4)), 4).alias("units_share"),
        F.percent_rank().over(w5).alias("units_percent_rank_2"),
    )
)

matched_keys = (
    enriched
    .select("product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("revenue") <= 3700)
        .select("product_id")
    )
)

verified_store_sales = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_store_sales = (
    verified_store_sales
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.store_name",
        "src.units_percent_rank",
        "src.units_share",
        F.col("d.category"),
    )
)

result = (
    enriched_store_sales
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "units_percent_rank",
        "units_share",
        F.col("min_inventory_quantity"),
    )
)
