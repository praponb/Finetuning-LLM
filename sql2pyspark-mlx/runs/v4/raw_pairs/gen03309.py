from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "store_id",
        F.last("store_id").over(w).alias("last_store_id"),
    )
)
