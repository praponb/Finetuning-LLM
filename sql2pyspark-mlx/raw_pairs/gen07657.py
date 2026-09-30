from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date"), "product_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy(F.month("sale_date"), "product_id")
w4 = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        F.avg("revenue").over(w).alias("revenue_avg_5_prior"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.avg("units").over(w3).alias("sale_date_month_avg_units"),
        F.lag("units", 2).over(w4).alias("prev_units"),
    )
)
