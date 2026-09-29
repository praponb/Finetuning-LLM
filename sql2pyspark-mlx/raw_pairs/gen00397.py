from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy("price", "id")
w2 = Window.partitionBy("category", "brand").orderBy("price", "id")
w3 = Window.partitionBy("brand").orderBy("cost_scaled", "id")
w4 = Window.partitionBy(F.month("launch_date"), "category").orderBy(F.col("price").desc())
w5 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-05-08").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        F.round((F.col("cost") * 0.9), 2).alias("cost_scaled"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "category",
        "price",
        "cost_scaled",
        "brand_filled",
        F.ntile(10).over(w).alias("price_bucket"),
        F.lag("cost_scaled", 1).over(w2).alias("prev_cost_scaled"),
        F.ntile(3).over(w3).alias("cost_scaled_bucket"),
        F.percent_rank().over(w4).alias("price_percent_rank"),
        F.last("brand", ignorenulls=True).over(w5).alias("last_known_brand"),
    )
)
