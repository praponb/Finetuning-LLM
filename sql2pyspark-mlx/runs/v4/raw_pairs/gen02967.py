from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("revenue")

with_windows = (
    df_store_sales
    .withColumn("running_revenue", F.sum("revenue").over(w_recent))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("revenue_bucket", F.ntile(3).over(w4))
    .withColumn("revenue_rank", F.rank().over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "running_revenue",
        "revenue_share",
    )
    .orderBy(F.col("revenue_share").desc_nulls_first())
)
