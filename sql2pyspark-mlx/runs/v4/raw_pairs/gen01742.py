from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")
w3 = Window.partitionBy("region", "store_id").orderBy("revenue", "id")
w4 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.region"), F.col("b.sqft"))
)

scored = (
    with_dims
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w)), 4))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w4))
)

joined = (
    scored
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.price"), F.col("b.category"), F.col("b.stock_qty"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        "store_id",
        F.when(F.col("revenue_share") >= 0.72, "HIGH").otherwise("LOW").alias("revenue_share_tier"),
    )
    .agg(
        F.avg(F.when(F.col("category") == "Home", F.col("price"))).alias("home_avg_price"),
        F.min("sqft").alias("min_sqft"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("store_id").asc_nulls_first())
)
