from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc_nulls_last())
w5 = Window.partitionBy("city").orderBy("price_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("stock_qty", "id")
w7 = Window.partitionBy("city", "category").orderBy(F.col("last_known_stock_qty").asc_nulls_first())

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-12-30").cast("date")) & F.col("supplier_id").isNotNull() & F.col("price").between(370, 1080))
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
    .withColumn("product_name_brand_key", F.concat_ws("/", "product_name", "brand"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id") == 40), "left_semi")
)

enriched = (
    eligible
    .select(
        "*",
        F.avg("days_since_launch_date").over(w).alias("days_since_launch_date_avg_2"),
        F.first("stock_qty", ignorenulls=True).over(w2).alias("first_known_stock_qty"),
        F.last("stock_qty", ignorenulls=True).over(w3).alias("last_known_stock_qty"),
        F.cume_dist().over(w4).alias("price_cume_dist"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"), F.col("p.city"))
)

enriched_products = (
    joined
    .select(
        "*",
        F.avg("days_since_launch_date").over(w5).alias("days_since_launch_date_avg_6"),
        F.lead("cost", 1).over(w6).alias("next_cost"),
        F.percent_rank().over(w7).alias("last_known_stock_qty_percent_rank"),
    )
)

enriched_products_2 = (
    enriched_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.city").alias("supplier_city"),
        F.col("d.supplier_name"),
        F.col("d.rating"),
    )
)

filtered = (
    enriched_products_2
    .filter((F.col("launch_date") > F.lit("2023-09-01").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "days_since_launch_date",
        "days_since_launch_date_avg_2",
        "first_known_stock_qty",
        "last_known_stock_qty",
        "country",
        "next_cost",
        "supplier_city",
        "supplier_name",
        "rating",
        F.substring("city", 1, 3).alias("city_prefix"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "supplier_city",
        F.col("next_cost").alias("NEXT_COST_VAL"),
        "rating",
        "price",
    )
    .unpivot(
        ["id", "supplier_city"],
        ["NEXT_COST_VAL", "rating", "price"],
        "kpi",
        "metric_value",
    )
)
