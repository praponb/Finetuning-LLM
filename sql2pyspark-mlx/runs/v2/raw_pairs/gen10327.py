from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "product_id",
        F.last("product_id").over(w).alias("last_product_id"),
        F.row_number().over(w2).alias("row_num"),
    )
)
