from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy(F.year("sale_date"), "store_id")
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").asc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        F.ntile(3).over(w).alias("revenue_bucket"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.cume_dist().over(w3).alias("revenue_cume_dist"),
        F.last("revenue", ignorenulls=True).over(w4).alias("last_known_revenue"),
    )
)
