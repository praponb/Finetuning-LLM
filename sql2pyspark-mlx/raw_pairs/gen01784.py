from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy(F.month("sale_date")).orderBy(F.col("revenue").desc_nulls_last())
w_recent = Window.partitionBy("store_id").orderBy("revenue", "id")
w5 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1900)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.ntile(4).over(w_hist).alias("revenue_bucket"),
        F.count("revenue").over(w2).alias("store_id_count_revenue"),
        F.dense_rank().over(w3).alias("revenue_dense_rank"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
