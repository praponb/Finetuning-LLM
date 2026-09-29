from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        "product_id",
        "store_id",
        "units",
        F.row_number().over(w).alias("row_num"),
        F.last("revenue").over(w2).alias("last_revenue"),
    )
)
