from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.orderBy(F.col("units").desc(), "id")

metrics = (
    df_store_sales
    .select(
        "*",
        F.ntile(5).over(w).alias("revenue_bucket"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.ntile(3).over(w3).alias("units_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "revenue_bucket",
        "corr_units_revenue",
    )
)
