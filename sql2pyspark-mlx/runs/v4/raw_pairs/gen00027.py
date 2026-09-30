from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("revenue") > 3700)
    .select(
        "id",
        "product_id",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"), F.col("p.stock_qty"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.cost"), F.col("p.stock_qty").alias("product_stock_qty"))
)

filtered_store_sales = (
    enriched
    .filter(F.col("product_id") == 4)
    .select(
        "*",
        (F.col("cost") - F.col("revenue")).alias("cost_minus_revenue"),
        F.datediff(F.current_date(), "sale_date_month_end").alias("days_since_sale_date_month_end"),
    )
)

by_type = (
    filtered_store_sales
    .groupBy("product_id")
    .pivot("category", ["Home", "Electronics", "Sports"])
    .agg(
        F.avg("days_since_sale_date_month_end").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
)

result = by_type.select("product_id", "Home_value_sum", "Home_cnt", "Sports_value_sum")
