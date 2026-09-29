from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w3 = Window.partitionBy(F.year("sale_date"), "store_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("product_id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("units").alias("sum_store_sale_units"))

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-31").cast("date"))
    .select("*", F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"))
)

with_windows = (
    valid
    .select(
        "*",
        F.lag("units", 2, 0).over(w).alias("prev_units"),
        F.cume_dist().over(w2).alias("units_cume_dist"),
        F.ntile(4).over(w3).alias("units_bucket"),
        F.corr("revenue", "units").over(w4).alias("corr_revenue_units"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .filter(~F.col("s.region").isin(["South", "East", "West"]))
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.revenue",
        "e.prev_units",
        "e.units_cume_dist",
        "e.corr_revenue_units",
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
)

enriched_store_sales = (
    enriched
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.city"),
        F.col("p.store_name").alias("store_store_name"),
        F.col("p.sqft"),
    )
)

joined = (
    enriched_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.prev_units",
        "src.store_name",
        "src.opened_date",
        "src.city",
        "src.sqft",
        F.col("p.opened_date").alias("store_opened_date"),
    )
)

enriched_store_sales_2 = (
    joined
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.prev_units",
        "e.opened_date",
        "e.city",
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
    )
)

result = (
    enriched_store_sales_2
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "opened_date",
        "city",
        "launch_date",
        "cost",
        F.when(F.col("city").rlike("^(London|Tokyo)$"), "OTHER_ZONE").otherwise("CORE").alias("city_zone"),
        F.col("sum_store_sale_units"),
    )
)
