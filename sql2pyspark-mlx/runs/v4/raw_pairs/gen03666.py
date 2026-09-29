from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("launch_date").desc(), "id")
w_part = Window.orderBy("cost", "id")
w4 = Window.orderBy(F.col("price").desc(), "id")

scored = (
    df_products
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("price_bucket", F.ntile(5).over(w4))
    .withColumn("cost_bucket", F.ntile(4).over(w_part))
)

result = (
    scored
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "supplier_id",
        "last_known_cost",
        "seq_no",
        "event_seq",
    )
    .orderBy(F.col("stock_qty").asc_nulls_last(), F.col("last_known_cost").desc())
)
