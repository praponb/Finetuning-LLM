from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "product_id",
        "units",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.round(F.avg("units").over(w2), 2).alias("units_moving_avg_7"),
    )
)
