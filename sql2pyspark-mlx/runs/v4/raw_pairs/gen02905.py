from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy("stock_qty", "id")
w5 = Window.partitionBy("category").orderBy("cumulative_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("category").orderBy("next_price", "id")
w8 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w9 = Window.partitionBy("supplier_id", "category").orderBy(F.col("next_price").desc(), "id")

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("launch_date") >= "2024-10-01"))
    .withColumn("launch_date_label", F.date_format("launch_date", "MMM yyyy"))
    .withColumn("product_name_category_key", F.concat_ws(",", "product_name", "category"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("OTHER")))
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("cost", 1).over(w).alias("prev_cost"),
        F.ntile(3).over(w2).alias("cost_bucket"),
        F.sum("stock_qty").over(w3).alias("cumulative_stock_qty"),
        F.lead("price", 1).over(w4).alias("next_price"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("cost").over(w5).alias("running_cost"),
        F.sum("cost_bucket").over(w6).alias("cumulative_cost_bucket"),
        F.lag("next_price", 1).over(w7).alias("prev_next_price"),
        F.ntile(5).over(w8).alias("price_bucket"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.rating"), F.col("p.country"))
)

result = (
    joined
    .groupBy("category", "brand")
    .pivot("country", ["DE", "TH"])
    .agg(F.max("rating").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
