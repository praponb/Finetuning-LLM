from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("running_count_cost").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

with_windows = (
    df_products
    .select(
        "id",
        "tags",
        "supplier_id",
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.revenue") >= 1800), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.revenue"))
)

result = (
    with_latest
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select("id", "tags", "supplier_id", "running_sum_cost", F.col("min_product_cost"))
)
