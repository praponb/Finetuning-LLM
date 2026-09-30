from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("running_stock_qty").desc(), "id")
w_part = Window.partitionBy("category", "brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

metrics = (
    df_products
    .withColumn("price_avg_2", F.avg("price").over(w_recent))
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w2))
)

metrics_products = (
    metrics
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("last_known_price_avg_2", F.last("price_avg_2", ignorenulls=True).over(w_part))
)

with_dims = (
    metrics_products
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.supplier_name"), F.col("d.rating"), F.col("d.city"))
)

result = (
    with_dims
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.col("min_product_cost"),
    )
    .orderBy(F.col("min_product_cost").desc_nulls_last())
)
