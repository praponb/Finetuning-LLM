from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("sale_date"), "product_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("units", "id")
w4 = Window.partitionBy(F.date_trunc("week", "sale_date"), "product_id").orderBy(F.col("units").desc(), "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_first())
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

with_windows = (
    df_store_sales
    .select(
        "*",
        F.avg("units").over(w).alias("units_avg_3"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.dense_rank().over(w5).alias("units_dense_rank"),
    )
)

result = (
    with_windows
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "row_seq",
        "event_seq",
        F.col("max_store_sale_revenue"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
