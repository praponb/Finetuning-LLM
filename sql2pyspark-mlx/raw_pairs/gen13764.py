from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        F.max("units").over(w).alias("product_id_max_units"),
        F.last("revenue", ignorenulls=True).over(w2).alias("last_known_revenue"),
        F.sum("units").over(w3).alias("running_units"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
