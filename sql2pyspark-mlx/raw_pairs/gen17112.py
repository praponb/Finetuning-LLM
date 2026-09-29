from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "city")
w2 = Window.partitionBy(F.year("launch_date"), "brand")
w3 = Window.partitionBy(F.date_trunc("month", "launch_date"), "country").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")
w5 = Window.partitionBy("city")
w6 = Window.orderBy("launch_date_month_avg_cost", "id")
w7 = Window.orderBy("price_share", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.orderBy(F.col("launch_date_month_avg_cost_bucket").desc(), "id")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-08").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("brand").isin(["Umbrella", "Initech"]), "Core").otherwise("Other").alias("brand_group"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "inner")
    .filter(F.col("b.country").isin(["SG", "DE"]))
    .select("s.*", F.col("b.city"), F.col("b.country"))
)

kept = (
    with_dims
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id") == 4), "left_semi")
)

enriched = (
    kept
    .select(
        "id",
        "brand",
        "price",
        "launch_date",
        "tags",
        "brand_group",
        "brand_clean",
        "city",
        F.avg("cost").over(w).alias("launch_date_month_avg_cost"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "brand",
        "price",
        "launch_date",
        "tags",
        "brand_group",
        "brand_clean",
        "price_share",
        F.ntile(10).over(w4).alias("price_bucket"),
        F.min("launch_date_month_avg_cost").over(w5).alias("city_min_launch_date_month_avg_cost"),
        F.ntile(10).over(w6).alias("launch_date_month_avg_cost_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.ntile(4).over(w4).alias("price_bucket_2"),
        F.avg("city_min_launch_date_month_avg_cost").over(w7).alias("city_min_launch_date_month_avg_cost_avg_2"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

result = (
    scored
    .select("id", "brand", "price_share", "price")
    .unpivot(
        ids=["id", "brand"],
        values=["price_share", "price"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
