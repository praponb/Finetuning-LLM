from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.corr("stock_qty", "price").over(w).alias("corr_stock_qty_price"),
        F.ntile(5).over(w2).alias("stock_qty_bucket"),
        F.sum("cost").over(w3).alias("cumulative_cost"),
    )
)

result = (
    enriched
    .groupBy("supplier_id")
    .agg(
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.percentile_approx("stock_qty", 0.95).alias("stock_qty_p95"),
    )
    .filter(F.col("stock_qty_p95") >= 0)
)
