from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date")).orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("month", "sale_date"), "product_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy("revenue", "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")

enriched = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        F.ntile(10).over(w).alias("revenue_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.corr("units", "revenue").over(w3).alias("corr_units_revenue"),
        F.ntile(5).over(w4).alias("revenue_bucket_2"),
        F.lag("revenue", 1).over(w5).alias("prev_revenue"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue_bucket",
        "revenue_bucket_2",
        "prev_revenue",
    )
    .orderBy(F.col("revenue_bucket").desc_nulls_first())
)
