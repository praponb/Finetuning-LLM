from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("units") > 29)
    .select(
        "id",
        "sale_date",
        "revenue",
        "store_id",
        "units",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_7"),
        F.row_number().over(w2).alias("units_row"),
    )
)
