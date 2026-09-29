from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "sale_date",
        "revenue",
        F.last("sale_date").over(w).alias("last_sale_date"),
        F.sum("revenue").over(w2).alias("store_id_sum_revenue"),
    )
)
