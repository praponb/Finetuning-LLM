from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "product_id",
        "units",
        F.row_number().over(w).alias("rn"),
        F.round(F.avg("units").over(w2), 2).alias("units_moving_avg_7"),
    )
    .orderBy("id")
)
