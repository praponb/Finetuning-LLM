from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-05-21").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        (F.col("price") - F.col("stock_qty")).alias("price_minus_stock_qty"),
        F.upper("brand").alias("brand_upper"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

result = (
    filtered
    .withColumn("cumulative_price", F.sum("price").over(w_ordered))
    .withColumn("cumulative_cost", F.sum("cost").over(w2))
)
