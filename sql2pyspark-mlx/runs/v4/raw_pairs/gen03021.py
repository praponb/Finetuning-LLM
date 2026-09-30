from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("brand").orderBy("revenue", "id")
w4 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category", "brand")
w6 = Window.partitionBy("supplier_id", "brand")
w7 = Window.partitionBy("category").orderBy("row_seq", "id")
w8 = Window.partitionBy("brand").orderBy("supplier_id_avg_units", "id")
w9 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("product_name_normalized", F.upper(F.trim("product_name")))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

latest_match_products = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.units"))
)

scored = (
    latest_match_products
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.first("units", ignorenulls=True).over(w4).alias("first_known_units"),
        F.sum("price").over(w5).alias("category_sum_price"),
        F.avg("units").over(w6).alias("supplier_id_avg_units"),
    )
)

result = (
    scored
    .select(
        "*",
        F.lag("stock_qty", 1).over(w7).alias("prev_stock_qty"),
        F.ntile(3).over(w8).alias("supplier_id_avg_units_bucket"),
        F.sum("row_seq").over(w9).alias("cumulative_row_seq"),
    )
)
