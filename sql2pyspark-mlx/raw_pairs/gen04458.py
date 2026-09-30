from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("launch_date"), "category").orderBy("price")
w3 = Window.partitionBy(F.date_trunc("month", "launch_date")).orderBy("cost", "id")

with_windows = (
    df_products
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w))
    .withColumn("price_dense_rank", F.dense_rank().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "tags",
        "first_known_cost",
        "price_dense_rank",
        "event_seq",
    )
    .orderBy(F.col("price_dense_rank").desc_nulls_first())
)
