from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("price").alias("avg_product_price"))

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-07-22").cast("date")) & F.col("brand").isNotNull())
    .select(
        "*",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

with_windows = (
    valid
    .withColumn("first_known_stock_qty", F.first("stock_qty", ignorenulls=True).over(w_running))
    .withColumn("cumulative_price", F.sum("price").over(w2))
)

matched_keys = (
    with_windows
    .select("brand")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isin([8, 13, 14, 18]))
        .select("brand")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        "launch_date_month_end",
        "brand_clean",
        "first_known_stock_qty",
        F.col("avg_product_price"),
    )
)
