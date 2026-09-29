from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "sale_date"), "store_id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("store_id") == 1)
    .select(
        "id",
        "sale_date",
        "store_id",
        "revenue",
        "units",
        F.avg("revenue").over(w).alias("sale_date_period_avg_revenue"),
        F.round(F.avg("units").over(w2), 2).alias("units_moving_avg_7"),
    )
)
