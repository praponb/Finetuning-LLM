from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("stock_qty").alias("min_product_stock_qty"))

enriched = (
    df_products
    .filter(F.month("launch_date").isin([3, 8, 10]))
    .select(
        "id",
        "category",
        "cost",
        "tags",
        "supplier_id",
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.max("cost").over(w).alias("running_max_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
        F.min("cost").over(w).alias("running_min_cost"),
        F.sum("price").over(w2).alias("running_price"),
    )
)

result = (
    enriched
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "cost",
        "tags",
        "supplier_id",
        "running_sum_cost",
        "running_max_cost",
        "running_count_cost",
        "running_min_cost",
        F.col("min_product_stock_qty"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("running_count_cost").desc())
)
