from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_int").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("corr_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("revenue") > 2700)
    .select(
        "*",
        F.ceil("units").alias("units_whole"),
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .withColumn("prev_units", F.lag("units").over(w2))
    .withColumn("next_units", F.lead("units").over(w2))
    .withColumn("next_revenue_int", F.lead("revenue_int").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w3))
)

with_windows = (
    metrics
    .withColumn("cumulative_corr_units_revenue", F.sum("corr_units_revenue").over(w4))
    .withColumn("running_revenue", F.sum("revenue").over(w5))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price").between(1010, 1040))
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.units",
        "s.units_whole",
        "s.revenue_int",
        "s.next_revenue_int",
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
)

result = enriched.select("id", "units", "next_revenue_int", "cost")
