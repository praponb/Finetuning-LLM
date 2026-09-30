from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-6, -1)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.last("units", ignorenulls=True).over(w_hist).alias("last_known_units"),
        F.sum("revenue").over(w2).alias("running_revenue"),
        F.percent_rank().over(w3).alias("units_percent_rank"),
        F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4).alias("revenue_share"),
        F.avg("revenue").over(w5).alias("revenue_avg_6_prior"),
    )
)
