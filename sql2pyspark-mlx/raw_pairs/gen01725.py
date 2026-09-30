from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        "units",
        "sale_date",
        F.row_number().over(w).alias("revenue_row"),
        F.last("sale_date").over(w2).alias("last_sale_date"),
    )
)
